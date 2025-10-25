# Rust JSONL Async

[![Crates.io](https://img.shields.io/crates/v/rust_jsonl_async.svg)](https://crates.io/crates/rust_jsonl_async)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

`rust_jsonl_async` est un wrapper Rust pour l'écriture de fichiers JSONL (JSON Lines) qui offre une interface thread-safe et non bloquante. Il exécute toutes les
opérations d'écriture dans un thread d'arrière-plan dédié, ce qui le rend idéal pour les applications qui nécessitent une journalisation (logging) performante sans
bloquer le thread principal.

Cette caisse est parfaite pour les applications à haute concurrence, comme les serveurs web ou les services de traitement de données, où la journalisation des événements
ou des objets structurés doit être rapide et efficace.

## Fonctionnalités

* **Écritures Asynchrones**: Les objets sérialisables sont envoyés à un thread d'écriture dédié via un canal, évitant ainsi de bloquer le thread appelant.
* **Écriture par Lots (Batching)**: Le worker utilise un buffer et traite les écritures par lots pour minimiser les opérations d'I/O système et maximiser le débit.
* **Sécurité des Threads (Thread-Safe)**: La structure `AsyncJsonlQueue` peut être partagée en toute sécurité entre plusieurs threads (`Sync` + `Send`).
* **Back-Pressure**: Le canal pour les écritures a une capacité limitée, ce qui empêche une utilisation excessive de la mémoire si les données sont produites plus
  rapidement qu'elles ne peuvent être écrites sur le disque.
* **Gestion du Cycle de Vie**: Le thread d'arrière-plan est démarré et arrêté proprement, avec des méthodes pour attendre la synchronisation des données. `Drop` est
  implémenté pour garantir que les données restantes sont écrites avant la fermeture.
* **Générique sur les Données**: Peut écrire n'importe quel type de données qui implémente `serde::Serialize`.

## Installation

Ajoutez cette ligne à votre `Cargo.toml`:

```toml
[dependencies]
rust_jsonl_async = "0.1.0"
```

*(Note: Remplacez `0.1.0` par la version souhaitée.)*

## Exemple d'Utilisation

Voici un exemple simple pour créer un fichier, y écrire des données structurées de manière asynchrone.

```rust
use rust_jsonl_async::AsyncJsonlQueue;
use serde::Serialize;
use std::time::Duration;
use tempfile::NamedTempFile;

#[derive(Serialize, Clone)]
struct LogEntry {
    level: String,
    message: String,
    timestamp: u64,
}

fn main() {
    // Utiliser un fichier temporaire pour cet exemple
    let temp_file = NamedTempFile::new().unwrap();
    let mut queue = AsyncJsonlQueue::new(temp_file.path());

    // Démarrer le thread d'écriture
    queue.start();

    // Attendre que le worker soit prêt (fichier ouvert)
    if !queue.wait_for_ready(Duration::from_secs(5)) {
        panic!("Le worker JSONL n'a pas pu démarrer à temps");
    }

    // Créer une entrée de log
    let entry = LogEntry {
        level: "info".to_string(),
        message: "Ceci est un test".to_string(),
        timestamp: 1678886400,
    };

    // Envoyer les données à la file d'écriture (ne bloque pas)
    queue.write(entry.clone()).unwrap();

    // `sync` attend que toutes les écritures en attente soient terminées
    assert!(queue.sync(Duration::from_secs(5)), "La synchronisation a échoué");

    // Lire le fichier pour vérifier le contenu
    let content = std::fs::read_to_string(temp_file.path()).unwrap();
    let written_line = content.lines().next().unwrap();

    let written_entry: LogEntry = serde_json::from_str(written_line).unwrap();
    assert_eq!(written_entry.message, "Ceci est un test");

    println!("Données écrites avec succès: {}", written_line);

    // La méthode `stop` est appelée automatiquement lorsque `queue` est détruite (Drop)
}
```

## API Principale

* `AsyncJsonlQueue::new(path)`: Crée une nouvelle file d'écriture pour le fichier spécifié.
* `queue.start()`: Démarre le thread d'écriture en arrière-plan.
* `queue.wait_for_ready(timeout)`: Attend que le thread worker ait ouvert le fichier et soit prêt à recevoir des données.
* `queue.write(data)`: Ajoute un objet sérialisable (`T: Serialize`) à la file d'écriture. Bloque si la file est pleine.
* `queue.sync(timeout)`: Bloque jusqu'à ce que toutes les écritures en attente dans la file soient écrites sur le disque.
* `queue.stop()`: Arrête proprement le thread d'arrière-plan (appelé automatiquement via `Drop`).

## Licence

Ce projet est sous licence MIT. Voir le fichier `LICENSE` pour plus de détails.
