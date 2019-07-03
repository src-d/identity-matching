import re

from idmatching.blacklist import IGNORED_DOMAIN, IGNORED_NAMES, IGNORED_TLD, IGNORED_EMAILS, \
    POPULAR_NAMES


def has_alphabetic_characters(string: str):
    return bool(re.search("[a-zA-Z]", string))


def is_blacklisted_name(name: str) -> bool:
    assert isinstance(name, str), "Expected string, got `%s` with type `%s`" % (name, type(name))
    return name.lower() in IGNORED_NAMES


def is_popular_name(name: str) -> bool:
    assert isinstance(name, str), "Expected string, got `%s` with type `%s`" % (name, type(name))
    return name.lower() in POPULAR_NAMES


def is_ignored_name(name: str):
    assert isinstance(name, str), "Expected string, got `%s` with type `%s`" % (name, type(name))
    return not has_alphabetic_characters(name) or \
           is_blacklisted_name(name)


def is_blacklisted_email(email: str) -> bool:
    assert isinstance(email, str), "Expected string, got `%s` with type `%s`" % (email,
                                                                                 type(email))
    return email.lower() in IGNORED_EMAILS


def is_blacklisted_domain(email: str) -> bool:
    return email.lower() in IGNORED_DOMAIN


def is_blacklisted_TLD(email: str) -> bool:
    assert isinstance(email, str), "Expected string, got `%s` with type `%s`" % (email,
                                                                                 type(email))
    return email.lower().split(".")[-1] in IGNORED_TLD


def is_single_word_domain(email: str) -> bool:
    assert isinstance(email, str), "Expected string, got `%s` with type `%s`" % (email,
                                                                                 type(email))
    return len(email.lower().split("@")[-1].split(".")) == 1


def is_multiple_emails(email: str) -> bool:
    assert isinstance(email, str), "Expected string, got `%s` with type `%s`" % (email,
                                                                                 type(email))
    return len(email.lower().split("@")) > 2


def is_ip_email(email: str) -> bool:
    assert isinstance(email, str), "Expected string, got `%s` with type `%s`" % (email,
                                                                                 type(email))
    last = email.lower().split("@")[-1]
    for part in last.split("."):
        if not str.isnumeric(part):
            return False
    return True


def is_noreply_github_email(email: str):
    assert isinstance(email, str), "Expected string, got `%s` with type `%s`" % (email,
                                                                                 type(email))
    if not is_multiple_emails(email) and email.endswith("users.noreply.github.com") and \
            email.lower().split("+")[0].isnumeric():
        return True
    return False


def is_ignored_email(email: str) -> bool:
    """
    Check if this email should be ignored.

    :param email: email to check.
    :return: True if it should be ignored. False if it can be used
    """
    email = email.strip()
    return "@" not in email or \
           is_blacklisted_email(email) or \
           is_blacklisted_domain(email) or \
           is_blacklisted_TLD(email) or \
           is_single_word_domain(email) or \
           is_multiple_emails(email) or \
           is_ip_email(email)
