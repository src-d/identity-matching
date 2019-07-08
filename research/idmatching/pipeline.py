from collections import defaultdict
import operator
import sys
from typing import Callable, List

from joblib import Memory
import pandas as pd
from tqdm import tqdm

from idmatching import strip_accents
from idmatching.filtering import is_ignored_name, is_ignored_email
from idmatching.people import RawPerson, identity_matching_pipeline


class CooccurrenceFiltering:
    """
    Class to count unique values per key given pairs (key, value) and filter them.
    """
    def __init__(self, threshold: int, preprocess_value: Callable = None,
                 preprocess_key: Callable = None, threshold_comp: str = ">=",
                 is_ignored_key: Callable = None, is_ignored_value: Callable = None):
        """
        Initialization.

        :param threshold: if number of emails per name will exceed this threshold than name will be
                          treated as popular.
        :param preprocess_key: function to preprocess key. `f(key: str) -> str`.
        :param preprocess_value: function to preprocess value. `f(value: str) -> str`.
        :param threshold_comp: operator to compare count with threshold. Accepted values: ">",
                               ">=", "<", "<=", "==".
        :param is_ignored_key: function to filter keys. `f(key: str) -> bool`. If True - key-value
                               will be ignored.
        :param is_ignored_value: function to filter values. `f(value: str) -> bool`. If True -
                                 key-value will be ignored.
        """
        assert threshold >= 0, "Threshold should be positive"
        assert isinstance(threshold, int), "Threshold should have type int but got %s" % \
                                           type(threshold)
        self.threshold = threshold
        if preprocess_value:
            self.preprocess_value = preprocess_value
        else:
            self.preprocess_value = lambda x: x
        if preprocess_key:
            self.preprocess_key = preprocess_key
        else:
            self.preprocess_key = lambda x: x
        self.operators = {">": operator.gt, "<": operator.lt, ">=": operator.ge, "<=": operator.le,
                          "==": operator.eq}
        assert threshold_comp in self.operators, "Unknown operator `%s`, accepted values `%s`" % (
            threshold_comp, self.operators.keys()
        )
        self.threshold_comp = threshold_comp

        self.is_ignored_key = is_ignored_key
        self.is_ignored_value = is_ignored_value

    def get_comparison_result(self, left, right):
        return self.operators[self.threshold_comp](left, right)

    def fit(self, keys: List[str], values: List[str]):
        """
        Fit on provided data.

        :param keys: list of keys for aggregation.
        :param values: list of values for counting unique values.
        :return: self.
        """
        err = "Lengths of names (%s) and emails (%s) are not the same" % (len(keys), len(values))
        assert len(keys) == len(values), err
        counter = defaultdict(set)
        for k, v in tqdm(zip(keys, values), total=len(keys)):
            if self.is_ignored_key(k) or self.is_ignored_value(v):
                continue
            counter[k].add(v)
        self.popular_keys = [k for k in counter if self.get_comparison_result(len(counter[k]),
                                                                              self.threshold)]
        return self


def prepare_is_blacklisted_function(black_list: List[str], preprocess_value: Callable = None) \
        -> Callable:
    if not preprocess_value:
        preprocess_value = lambda x: x
    blackset = set(black_list)

    def is_blacklisted(value):
        return preprocess_value(value) in blackset

    return is_blacklisted


def read_names_emails_gids(data_loc: str, use_committer: bool = False):
    data = pd.read_csv(data_loc)
    if not use_committer:
        data = data[["author.email", "author.name", "author_id", "repository"]]
    data.dropna(inplace=True)
    for col_name in data.columns:
        if col_name == "author_id":
            continue
        data[col_name] = data[col_name].map(
            lambda x: " ".join(strip_accents(x).strip().lower().split()))
    print("Data shape is", data.shape)

    emails = data["author.email"].tolist()
    names = data["author.name"].tolist()
    github_ids = data["author_id"].tolist()
    repositories = data["repository"].tolist()
    if use_committer:
        emails.extend(data["committer.email"].tolist())
        names.extend(data["committer.name"].tolist())
        github_ids.extend(data["committer_id"].tolist())
    return names, emails, github_ids, repositories


def get_preprocessing(lower: bool):
    if lower:
        return lambda x: ' '.join(x.strip().split()).lower()
    return lambda x: ' '.join(x.strip().split())


def pipeline(name_threshold: int, email_threshold: int, data_loc: str,
             lower_names: bool = True, lower_emails: bool = True,
             use_committer: bool = False, cache_loc: str = None,
             use_precalculated_popular: bool = True, debug_output = None):
    if cache_loc:
        memory = Memory(cache_loc, verbose=0)
        read_names_emails_gids_cache = memory.cache(read_names_emails_gids)
    else:
        read_names_emails_gids_cache = read_names_emails_gids
    names, emails, github_ids, repositories = read_names_emails_gids_cache(
        data_loc=data_loc, use_committer=use_committer
    )
    print("Input is ready! Number of samples", len(names))
    # prepare preprocessing function for names and emails
    preproces_emails = get_preprocessing(lower_emails)
    preproces_names = get_preprocessing(lower_names)

    names = list(map(preproces_names, names))
    emails = list(map(preproces_emails, emails))

    if use_precalculated_popular:
        from idmatching.blacklist import POPULAR_NAMES, POPULAR_EMAILS
        popular_names = POPULAR_NAMES
        popular_emails = POPULAR_EMAILS
    else:
        # collect popular names and emails
        popular_names = CooccurrenceFiltering(
            threshold=name_threshold, threshold_comp=">=",
            is_ignored_key=is_ignored_name, is_ignored_value=is_ignored_email
        ).fit(names, emails).popular_keys

        popular_emails = CooccurrenceFiltering(
            threshold=email_threshold, threshold_comp=">=",
            is_ignored_key=is_ignored_email, is_ignored_value=is_ignored_name
        ).fit(emails, names).popular_keys
    print("Number of popular names to ignore", len(popular_names))
    print("Number of popular emails to ignore", len(popular_emails))

    # prepare filtering functions
    is_ignored_popular_name = prepare_is_blacklisted_function(black_list=popular_names,
                                                              preprocess_value=preproces_names)
    is_ignored_popular_email = prepare_is_blacklisted_function(black_list=popular_emails,
                                                               preprocess_value=preproces_emails)

    # replace popular names with (name, repository) pair
    for i, (name, repository) in enumerate(zip(names, repositories)):
        if is_ignored_popular_name(name):
            names[i] = "(%s, %s)" % (name, repository)

    popular_names = CooccurrenceFiltering(
        threshold=name_threshold, threshold_comp=">=",
        is_ignored_key=is_ignored_name, is_ignored_value=is_ignored_email
    ).fit(names, emails).popular_keys
    #print("Number of popular names to ignore after replacement", len(popular_names))
    #is_ignored_popular_name = prepare_is_blacklisted_function(black_list=popular_names,
    #                                                          preprocess_value=preproces_names)
    is_ignored_popular_name = lambda *args: False


    # identity matching
    raw_persons = []
    for name, email in tqdm(zip(names, emails), total=len(names)):
        raw_persons.append(RawPerson(name=preproces_names(name),
                                     email=preproces_emails(email)))

    identity2person = identity_matching_pipeline(raw_persons=raw_persons,
                                                 is_ignored_name=is_ignored_name,
                                                 is_ignored_email=is_ignored_email,
                                                 is_popular_name=is_ignored_popular_name,
                                                 is_popular_email=is_ignored_popular_email)

    # save result
    identity2person_to_save = sorted(
        "%s||%s\n" % ("|".join(sorted(person.names)), "|".join(sorted(person.emails)))
        for person in identity2person.values())
    if debug_output:
        with open(debug_output, "w") as f:
            f.writelines(identity2person_to_save)
    # evaluation
    # predicted
    name_emails2id = {}
    for k, v in tqdm(identity2person.items(), total=len(identity2person)):
        for em in v.emails:
            name_emails2id[em] = k
        for n in v.names:
            name_emails2id[n] = k
    # ground truth
    email_name2gid = {}
    gid2email_name = defaultdict(set)
    for name, email, gid in tqdm(zip(names, emails, github_ids),
                                 total=len(names)):
        name, email = preproces_names(name), preproces_emails(email)
        if not is_ignored_name(name) and not is_ignored_email(
                email):
            email_name2gid[name] = gid
            email_name2gid[email] = gid
            gid2email_name[gid].add(name)
            gid2email_name[gid].add(email)

    # measure quality per sample
    prec = []
    rec = []
    f1 = []
    cc_size = []

    for person_names_emails in tqdm(gid2email_name.values(), total=len(gid2email_name)):
        pred_id = set()
        for ent in person_names_emails:
            pred_id.add(name_emails2id[ent])
        for pid in pred_id:
            intersection = 0
            for ent in person_names_emails:
                if ent in identity2person[pid].emails or ent in identity2person[pid].names:
                    intersection += 1
            rec.append(intersection / len(person_names_emails))
            prec.append(intersection /
                        (len(identity2person[pid].emails) + len(identity2person[pid].names))
                        )

            if prec[-1] == 0 and rec[-1] == 0:
                f1.append(0)
            else:
                f1.append(2 * prec[-1] * rec[-1] / (prec[-1] + rec[-1]))
            cc_size.append(len(identity2person[pid].emails) + len(identity2person[pid].names))

    avr = lambda x: sum(x) / len(x)
    avr_prec, avr_rec, avr_f1 = avr(prec), avr(rec), avr(f1)
    print("Precision %s, recall %s, f1 %s" % (avr_prec, avr_rec, avr_f1))

    wavr = lambda x, w: sum(x_ * w_ for x_, w_ in zip(x, w)) / sum(w)
    wavr_prec, wavr_rec, wavr_f1 = wavr(prec, cc_size), wavr(rec, cc_size), wavr(f1, cc_size)
    print("Precision %s, recall %s, f1 %s" % (wavr_prec, wavr_rec, wavr_f1))

    return avr_prec, avr_rec, avr_f1, wavr_prec, wavr_rec, wavr_f1, identity2person, \
           gid2email_name, raw_persons


if __name__ == '__main__':
    # The Argument is the local path to the file at https://jupyter.k8s.ml.prod.srcd.run
    # And the path on the server is egorbu/idmatching/data/aggregated_deduplicated.csv
    data_loc = sys.argv[1]
    print("Running on aggregated data at %s" % data_loc)
    cache_loc = "../../../cache"
    res = pipeline(name_threshold=5, email_threshold=28, data_loc=data_loc,
                   cache_loc=None, debug_output="python_result")
