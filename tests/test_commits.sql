CREATE DATABASE IF NOT EXISTS gitbase;

USE gitbase;

CREATE TABLE IF NOT EXISTS commits (
	repository_id NVARCHAR(50) NOT NULL,
	commit_author_name NVARCHAR(50) NOT NULL,
	commit_author_email NVARCHAR(50) NOT NULL
);

INSERT INTO commits (repository_id, commit_author_name, commit_author_email)
VALUES
	("repo1", "bob", "bob@google.com"),
	("repo2", "bob", "bob@google.com"),
	("repo1", "alice", "alice@google.com"),
	("repo1", "bob", "bob@google.com"),
	("repo1", "bob", "bad-email@domen"),
	("repo1", "admin", "someone@google.com");