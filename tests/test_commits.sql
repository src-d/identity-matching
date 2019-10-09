CREATE DATABASE IF NOT EXISTS gitbase;

USE gitbase;

CREATE TABLE IF NOT EXISTS commits (
	repository_id NVARCHAR(50) NOT NULL,
	commit_author_name NVARCHAR(50) NOT NULL,
	commit_author_email NVARCHAR(50) NOT NULL,
	commit_author_when DATETIME NOT NULL
);

INSERT INTO commits (repository_id, commit_author_name, commit_author_email, commit_author_when)
VALUES
	("repo1", "bob", "bob@google.com", "2019-01-01 00:00:00"),
	("repo2", "bob", "bob@google.com", "2019-02-01 02:00:00"),
	("repo1", "alice", "alice@google.com", "2019-04-20 10:06:02"),
	("repo1", "bob", "bob@google.com", "2019-04-01 17:00:00"),
	("repo1", "bob", "bad-email@domen", "2019-03-01 20:05:00"),
	("repo1", "admin", "someone@google.com", "2019-02-20 13:39:00");