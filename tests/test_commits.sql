CREATE DATABASE IF NOT EXISTS gitbase;

USE gitbase;

CREATE TABLE IF NOT EXISTS commits (
	repository_id NVARCHAR(50) NOT NULL,
	commit_author_name NVARCHAR(50) NOT NULL,
	commit_author_email NVARCHAR(50) NOT NULL,
    commit_hash NVARCHAR(40) NOT NULL,
	commit_author_when DATETIME NOT NULL
);

INSERT INTO commits (repository_id, commit_author_name, commit_author_email, commit_hash, commit_author_when)
VALUES
	("repo1", "bob", "bob@google.com", "aaa", "2019-01-01 00:00:00"),
	("repo2", "bob", "bob@google.com", "bbb", "2019-02-01 02:00:00"),
	("repo1", "alice", "alice@google.com", "ccc", "2019-04-20 10:06:02"),
	("repo1", "bob", "bob@google.com", "ddd", "2019-04-01 17:00:00"),
	("repo1", "bob", "bad-email@domen", "eee", "2019-03-01 20:05:00"),
	("repo1", "admin", "someone@google.com", "fff", "2019-02-20 13:39:00");