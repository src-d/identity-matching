from collections import defaultdict
from typing import Callable, Mapping, NamedTuple, List, Set

from tqdm import tqdm
tqdm = lambda *i, **kwargs: i[0]

from idmatching.filtering import is_ignored_email, is_ignored_name


class Person(NamedTuple):
    """
    Person is a single individual that can have multiple names and emails.
    """
    id: int
    names: Set[str]
    emails: Set[str]

    def __repr__(self):
        return "|".join(map(str, self.names | self.emails))


class RawPerson(NamedTuple):
    """
    RawPerson is a single entry from initial data - name & email.
    """
    name: str
    email: str

    def __repr__(self):
        return "|".join(self.names + self.emails)


class WeightedQuickUnionPathCompressionUF:
    """
    Class that implements functionality for connected components search.
    """

    def __init__(self, n_components: int = 0):
        self.parent = []
        self.size = []
        for i in range(n_components):
            self.add_component()

    def n_components(self):
        assert len(self.parent) == len(self.size), \
            "n_parents (%s) != n_sizes (%s)" % (len(self.parent), len(self.size))
        return len(self.parent)

    def add_component(self):
        """
        Add new component.
        """
        self.parent.append(len(self.parent))  # parent is itself
        self.size.append(1)

    def union(self, id1: int, id2: int):
        """
        Union 2 components with indexes id1 & id2.

        :param id1: index of first component.
        :param id2: index of second component.
        """
        p1 = self.find(id1)
        p2 = self.find(id2)
        if p1 == p2:
            return  # nothing to do - common parent already
        # put smallest subtree below biggest
        if self.size[p1] > self.size[p2]:
            self.parent[p2] = p1
            self.size[p1] += self.size[p2]
        else:
            self.parent[p1] = p2
            self.size[p2] += self.size[p1]

    def validate(self, index: int) -> bool:
        """
        Check that index is valid. If not - raise ValueError.

        :param index: index to check.
        """
        if not (0 <= index < len(self.parent))  or not (type(index) == int):
            raise ValueError("Not valid index %s with type %s, size of parents list is %s" %
                             (index, type(index), len(self.parent)))

    def find(self, index: int) -> int:
        """
        Find parent for given index.

        :param index: index of element to search.
        :return: index of parent.
        """
        self.validate(index)
        root = index
        while root != self.parent[root]:
            self.parent[root] = self.parent[self.parent[root]]
            root = self.parent[root]
        return root

    def connected(self, id1: int, id2: int) -> bool:
        """
        Check if 2 components are connected.

        :param id1: index of first component.
        :param id2: index of second component.
        :return: True if connected and False if not connected.
        """
        return self.find(id1) == self.find(id2)


class People(dict):
    """
    People is a collection to store RawPersons.
    """
    def __init__(self, raw_persons=None, is_ignored_name: Callable = is_ignored_name,
                 is_ignored_email: Callable = is_ignored_email):
        self.is_ignored_name = is_ignored_name
        self.is_ignored_email = is_ignored_email
        self._people = {}
        self.uf = WeightedQuickUnionPathCompressionUF()
        self.email2id = defaultdict(set)
        self.name2id = defaultdict(set)
        if raw_persons:
            self.update(raw_persons)



    @property
    def people(self):
        return self._people

    def update(self, raw_persons: List[RawPerson]):
        """
        Update People container given new raw_persons.
        :param raw_persons: list of raw persons.
        """
        for p in raw_persons:
            self.add_raw_person(p)

    def add_raw_person(self, raw_person: RawPerson):
        if self.is_ignored_name(raw_person.name) or self.is_ignored_email(raw_person.email):
            return
        if raw_person not in self.people:
            self.people[raw_person] = len(self.people)
            self.uf.add_component()
            assert self.people[raw_person] == len(self.uf.parent) - 1, "%s %s" % (
                    self.people[raw_person], len(self.uf.parent) - 1
            )
            self.email2id[raw_person.email].add(len(self.people))
            self.name2id[raw_person.name].add(len(self.people))

    def __getitem__(self, item):
        assert isinstance(item, int), "Expected integer key, got `%s`" % type(item)
        return self._people[item]

    def __setitem__(self, key, value):
        assert isinstance(key, int), "Expected integer key, got `%s`" % type(key)
        assert isinstance(value, Person), "Expected class Person, got `%s`" % type(value)
        self._people[key] = value

    @staticmethod
    def create_reversed_index(people: Mapping[int, Person], attribute_name: str) \
            -> Mapping[str, Set[int]]:
        """
        Create mapping person attribute to unique indexes of people.

        :param people: dictionary of people ids to peoples.
        :param attribute_name: attribute to use (email/name).
        :return: dictionary.
        """
        entity2ids = defaultdict(set)
        for person, index in people.items():
            entity2ids[getattr(person, attribute_name)].add(index)
        return entity2ids


def identity_matching_pipeline(raw_persons: List[RawPerson],
                               is_popular_name: Callable, is_popular_email: Callable,
                               is_ignored_name: Callable = is_ignored_name,
                               is_ignored_email: Callable = is_ignored_email):
    """
    Identity matching pipeline - exact match by emails & names.
    :param raw_persons:
    :return:
    """
    print("Initialization in progress...")
    people = People(raw_persons=raw_persons, is_ignored_name=is_ignored_name,
                    is_ignored_email=is_ignored_email)
    print("Initialization completed!")
    # Connect components based on emails
    print("Email part in progress...")
    email2id = people.create_reversed_index(people.people, "email")
    for em, group in tqdm(email2id.items(), total=len(email2id)):
        if is_popular_email(em):
            continue
        for elem in group:
            break
        for index in group:
            people.uf.union(elem, index)
    del email2id
    print("Email part completed!")
    # Connect components based on names
    print("Name part in progress...")
    # name2id = people.create_reversed_index(people.people, "name")
    # for n, group in tqdm(name2id.items(), total=len(name2id)):
    #     if is_popular_name(n):
    #         continue
    #     for elem in group:
    #         break
    #     for index in group:
    #         people.uf.union(elem, index)
    # del name2id
    print("Name part completed!")
    # Use connected components to create final identities
    # all RawPersons with the same parent will go to the same identity
    print("Preparation of results...")
    identity2person = defaultdict(Person)
    for raw_person, index in people.people.items():
        parent_index = people.uf.find(index)
        if parent_index not in identity2person:
            identity2person[parent_index] = Person(id=parent_index,
                                                   emails=set([raw_person.email]),
                                                   names=set([raw_person.name]),
                                                   )
        else:
            identity2person[parent_index].emails.add(raw_person.email)
            identity2person[parent_index].names.add(raw_person.name)
    print("Preparation of results completed!")
    return identity2person
