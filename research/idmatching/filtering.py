import re
import unicodedata

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
    return is_blacklisted_name(name)


def is_blacklisted_email(email: str) -> bool:
    assert isinstance(email, str), "Expected string, got `%s` with type `%s`" % (email,
                                                                                 type(email))
    return email.lower() in IGNORED_EMAILS


def is_blacklisted_domain(email: str) -> bool:
    return email.split("@")[-1].lower() in IGNORED_DOMAIN


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


isIP4EmailRegex = re.compile(r"\d+\.\d+\.\d+\.\d+$")
isIP6EmailRegex = re.compile(
    r"(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}"
    r":[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}("
    r":[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:)"
    r"{1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,"
    r"4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0"
    r"-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-f"
    r"A-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-"
    r"9]){0,1}[0-9]))")


def is_ip_email(email: str) -> bool:
    assert isinstance(email, str), "Expected string, got `%s` with type `%s`" % (email,
                                                                                 type(email))
    last = email.lower().split("@")[-1]
    return isIP4EmailRegex.search(last) is not None or isIP6EmailRegex.search(last) is not None


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
