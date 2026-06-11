-- Créé automatiquement au premier démarrage de PostgreSQL
-- Ajoutez ici vos scripts SQL d'initialisation

CREATE TABLE IF NOT EXISTS events (
    id          SERIAL PRIMARY KEY,
    topic       VARCHAR(255) NOT NULL,
    payload     JSONB,
    created_at  TIMESTAMP DEFAULT NOW()
);
