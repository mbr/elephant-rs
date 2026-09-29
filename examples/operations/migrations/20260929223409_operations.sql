CREATE SCHEMA operations;

CREATE TABLE operations.requests (
    entry_id uuid PRIMARY KEY,
    amount bigint NOT NULL,
    task_id uuid NOT NULL
);

CREATE TABLE operations.postings (
    entry_id uuid PRIMARY KEY REFERENCES operations.requests(entry_id),
    amount bigint NOT NULL
);
