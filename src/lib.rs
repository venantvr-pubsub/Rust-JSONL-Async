use crossbeam_channel::{self, Receiver, Sender};
use log::{error, info};
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Error as IoError, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

// Capacité maximale de la file d'attente avant que `write` ne devienne bloquant.
const CHANNEL_CAPACITY: usize = 10000;

/// Erreur d'envoi de tâche d'écriture
#[derive(Debug)]
pub struct WriteError;

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Failed to send write task (worker thread terminated)")
    }
}

impl std::error::Error for WriteError {}

// --- AMÉLIORATION 1 ---
// Trait pour l'effacement de type (type erasure)
trait SerializableTask: Send + 'static {
    fn serialize_into(self: Box<Self>, writer: &mut BufWriter<File>) -> Result<(), IoError>;
}

// Implémentation pour tout type sérialisable
impl<T: Serialize + Send + 'static> SerializableTask for T {
    fn serialize_into(self: Box<Self>, writer: &mut BufWriter<File>) -> Result<(), IoError> {
        // Sérialise directement dans le writer, pas de String intermédiaire
        serde_json::to_writer(writer.by_ref(), &*self)?;
        // Ajoute le saut de ligne
        writer.write_all(b"\n")?;
        Ok(())
    }
}

// Tâches que le thread worker peut recevoir
// --- CORRIGÉ : #[derive(Clone)] supprimé ---
enum JsonlTask {
    // N'est plus un Arc<str>, mais un trait object qui sait se sérialiser
    Write(Box<dyn SerializableTask>),
    Sync(Sender<()>),
    Stop,
}

// Actions que le worker peut entreprendre après avoir traité une tâche
#[derive(PartialEq)]
enum WorkerAction {
    Continue,
    Stop,
}

// État partagé pour le signal "ready"
type ReadyState = Arc<(Mutex<bool>, Condvar)>;

/// Gère un fichier JSONL avec un thread d'écriture dédié.
pub struct AsyncJsonlQueue {
    file_path: PathBuf,
    task_sender: Option<Sender<JsonlTask>>,
    writer_thread: Option<JoinHandle<Result<(), IoError>>>,
    ready_state: ReadyState,
}

impl AsyncJsonlQueue {
    /// Crée un nouveau gestionnaire de file JSONL.
    /// N'active pas le thread worker avant l'appel à .start().
    pub fn new(file_path: impl AsRef<Path>) -> Self {
        Self {
            file_path: file_path.as_ref().to_path_buf(),
            task_sender: None,
            writer_thread: None,
            ready_state: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }

    /// Démarre le thread worker en arrière-plan.
    pub fn start(&mut self) {
        if self.writer_thread.is_some() {
            return; // Déjà démarré
        }

        let (task_sender, task_receiver) =
            crossbeam_channel::bounded::<JsonlTask>(CHANNEL_CAPACITY);
        let file_path = self.file_path.clone();
        let ready_state = Arc::clone(&self.ready_state);

        let writer_thread = thread::Builder::new()
            .name("AsyncJsonlWorker".to_string())
            .spawn(move || jsonl_worker_fn(file_path, task_receiver, ready_state))
            .expect("Failed to spawn JSONL worker thread");

        self.task_sender = Some(task_sender);
        self.writer_thread = Some(writer_thread);
    }

    /// Attend que le thread worker ait initialisé le fichier.
    pub fn wait_for_ready(&self, timeout: Duration) -> bool {
        let (lock, cvar) = &*self.ready_state;
        let ready = lock.lock().unwrap();
        if *ready {
            return true;
        } // Vérification rapide
        cvar.wait_timeout_while(ready, timeout, |ready_val| !*ready_val)
            .unwrap()
            .1
            .timed_out()
            == false
    }

    /// Arrête proprement le thread worker.
    /// Appelé automatiquement lorsque `AsyncJsonlQueue` est "drop" (sort du scope).
    pub fn stop(&mut self) {
        if let Some(thread) = self.writer_thread.take() {
            info!("Stopping JSONL worker...");
            if let Some(sender) = self.task_sender.as_ref() {
                // S'assurer que la file est vide avant d'envoyer 'Stop'
                let (sync_tx, sync_rx) = crossbeam_channel::bounded(1);
                sender.send(JsonlTask::Sync(sync_tx)).ok();
                sync_rx.recv_timeout(Duration::from_secs(5)).ok();

                // Envoyer le signal d'arrêt
                sender.send(JsonlTask::Stop).ok();
            }
            // Attendre la fin du thread
            thread.join().expect("Worker thread panicked").ok();
            info!("Worker stopped.");
        }
        self.task_sender = None;
    }

    /// Attend que toutes les écritures actuellement dans la file soient terminées.
    pub fn sync(&self, timeout: Duration) -> bool {
        // Canal 'one-shot' pour cet événement de synchro
        let (sync_tx, sync_rx) = crossbeam_channel::bounded(1);
        if let Some(sender) = self.task_sender.as_ref() {
            if sender.send(JsonlTask::Sync(sync_tx)).is_err() {
                return false; // Le thread worker est mort
            }
            // Attendre la notification de retour
            sync_rx.recv_timeout(timeout).is_ok()
        } else {
            false
        }
    }

    /// Ajoute des données sérialisables à la file.
    /// **Bloque si la file est pleine** (capacité = `CHANNEL_CAPACITY`).
    ///
    /// --- NOTE (Breaking Change) ---
    /// Prend 'data' par VALEUR (T) et non par référence (&T)
    pub fn write<T: Serialize + Send + 'static>(&self, data: T) -> Result<(), WriteError> {
        // Pas de sérialisation ici.
        // Crée le trait object, une opération très rapide (allocation Box).
        let task = JsonlTask::Write(Box::new(data));

        self.task_sender
            .as_ref()
            .ok_or(WriteError)?
            .send(task)
            .map_err(|_| WriteError)
    }
}

/// Gère la fermeture propre du thread lorsque l'objet `AsyncJsonlQueue` est détruit.
impl Drop for AsyncJsonlQueue {
    fn drop(&mut self) {
        self.stop();
    }
}

// --- WORKER ENTIÈREMENT REFACTORISÉ POUR LE BATCHING ---

/// La fonction exécutée par le thread worker.
fn jsonl_worker_fn(
    file_path: PathBuf,
    receiver: Receiver<JsonlTask>,
    ready_state: ReadyState,
) -> Result<(), IoError> {
    // 1. Créer le répertoire parent s'il n'existe pas
    if let Some(parent) = file_path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }

    // 2. Ouvrir le fichier en mode 'append' et 'create'
    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&file_path)?;

    // 3. Utiliser BufWriter pour le batching automatique des I/O
    let mut writer = BufWriter::new(file);

    info!("JSONL worker is ready for file: {:?}", file_path);

    // 4. Signaler que le fichier est prêt
    {
        let (lock, cvar) = &*ready_state;
        let mut ready = lock.lock().unwrap();
        *ready = true;
        cvar.notify_one();
    }

    // 5. Boucle principale de traitement par lots
    let mut sync_notifiers: Vec<Sender<()>> = Vec::new();

    // Attend la première tâche (bloquant)
    while let Ok(first_task) = receiver.recv() {
        sync_notifiers.clear();

        // Traiter le lot dans une closure pour la gestion d'erreur
        let batch_result: Result<WorkerAction, IoError> = (|| {
            // 1. Traiter la première tâche
            let mut action = process_task(first_task, &mut writer, &mut sync_notifiers)?;
            if action == WorkerAction::Stop {
                return Ok(WorkerAction::Stop);
            }

            // 2. Vider la file (non-bloquant) et traiter le reste du lot
            while let Ok(task) = receiver.try_recv() {
                action = process_task(task, &mut writer, &mut sync_notifiers)?;
                if action == WorkerAction::Stop {
                    return Ok(WorkerAction::Stop);
                }
            }

            // --- AMÉLIORATION 2 ---
            // Le flush() est supprimé d'ici. Il est géré par la tâche Sync
            // ou lorsque le BufWriter est plein.
            Ok(WorkerAction::Continue)
        })(); // Fin de la closure

        // 4. Notifier tous les appelants de `sync()` APRES le flush (qui a eu lieu dans process_task)
        for notify in sync_notifiers.drain(..) {
            notify.send(()).ok(); // Ignorer l'erreur si le receveur est parti
        }

        match batch_result {
            Ok(WorkerAction::Stop) => break,  // Sortir de la boucle 'while let'
            Ok(WorkerAction::Continue) => (), // Continuer à la prochaine boucle
            Err(e) => {
                error!("JSONL worker batch failed. Data may be lost. Error: {}", e);
            }
        }
    }

    info!("JSONL worker shutting down.");
    writer.flush()?; // Un dernier flush avant de fermer
    Ok(())
}

/// Fonction helper pour traiter une seule tâche
fn process_task(
    task: JsonlTask,
    writer: &mut BufWriter<File>,
    notifiers: &mut Vec<Sender<()>>,
) -> Result<WorkerAction, IoError> {
    match task {
        JsonlTask::Write(task) => {
            // --- AMÉLIORATION 1 ---
            // La sérialisation a lieu ici, directement dans le buffer
            task.serialize_into(writer)?;
        }
        JsonlTask::Sync(notify) => {
            // --- AMÉLIORATION 2 ---
            // C'est MAINTENANT qu'on flushe, uniquement sur demande de sync
            writer.flush()?;
            notifiers.push(notify);
        }
        JsonlTask::Stop => {
            return Ok(WorkerAction::Stop);
        }
    }
    Ok(WorkerAction::Continue)
}

// ---
// TESTS (Mis à jour pour la nouvelle API)
// ---

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;
    use std::time::Duration;
    use tempfile::NamedTempFile;

    // Équivalent du fixture 'manager'
    fn setup_queue() -> (AsyncJsonlQueue, NamedTempFile) {
        let temp_file = NamedTempFile::new().unwrap();
        let mut queue = AsyncJsonlQueue::new(temp_file.path());
        queue.start();
        assert!(
            queue.wait_for_ready(Duration::from_secs(5)),
            "Le worker n'a pas pu démarrer à temps."
        );
        (queue, temp_file)
    }

    fn read_lines(path: &Path) -> Vec<String> {
        let content = fs::read_to_string(path).unwrap_or_default();
        content.lines().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_initialization() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();
        let instance = AsyncJsonlQueue::new(path.clone());
        assert_eq!(instance.file_path, path);
        assert!(instance.writer_thread.is_none());
    }

    #[test]
    fn test_lifecycle_start_stop() {
        let (mut queue, _temp_file) = setup_queue();
        assert!(queue.writer_thread.is_some());
        queue.stop();
        assert!(queue.writer_thread.is_none());
    }

    #[test]
    fn test_single_write_operation() {
        let (mut queue, temp_file) = setup_queue();
        let test_data = json!({"event_id": 1, "message": "hello world"});

        // CORRIGÉ: `test_data.clone()` pour passer la possession
        // tout en la gardant pour l'assertion plus bas.
        queue.write(test_data.clone()).unwrap();
        queue.stop(); // Stoppe et flushe (via le sync interne de stop)

        let lines = read_lines(temp_file.path());
        assert_eq!(lines.len(), 1);
        let read_data: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        assert_eq!(read_data, test_data);
    }

    #[test]
    fn test_concurrent_writes() {
        let (queue, temp_file) = setup_queue();
        let queue_arc = Arc::new(queue);

        let num_threads = 10;
        let writes_per_thread = 50;
        let total_writes = num_threads * writes_per_thread;
        let mut threads = vec![];

        for i in 0..num_threads {
            let queue_clone = Arc::clone(&queue_arc);
            threads.push(thread::spawn(move || {
                for j in 0..writes_per_thread {
                    // CORRIGÉ: Le `json!` macro crée une valeur, on la déplace
                    queue_clone
                        .write(json!({"thread_id": i, "iteration": j}))
                        .unwrap();
                }
            }));
        }

        for thread in threads {
            thread.join().unwrap();
        }

        // Dropper l'Arc force l'appel à Drop -> stop()
        drop(queue_arc);

        let lines = read_lines(temp_file.path());
        assert_eq!(lines.len(), total_writes);
    }

    #[test]
    fn test_drop_flushes_data() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();
        let test_data = json!({"context": "success"});

        {
            let mut queue = AsyncJsonlQueue::new(path.clone());
            queue.start();
            assert!(
                queue.wait_for_ready(Duration::from_secs(5)),
                "Le worker n'a pas pu démarrer à temps."
            );

            // CORRIGÉ: `test_data.clone()` pour passer la possession
            queue.write(test_data.clone()).unwrap();

            // 'queue' est détruit ici, appelant drop() et stop()
        }

        let lines = read_lines(&path);
        assert_eq!(lines.len(), 1);
        let read_data: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        assert_eq!(read_data, test_data);
    }
}
