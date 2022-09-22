""" Datastore module """

from abc import ABC, abstractmethod


class Datastore(ABC):
    """ Abstract class to represent the datastore (or warehouse) """

    @abstractmethod
    def get_previous_instance(
            self, orderby: str,
            current_ins: dict[str, str]) -> dict[str, str] | bool:
        """ Abstract method to return the previous instance of the specified record
            Override this method to retrieve the records from the desired datastore/warehouse

        Args:
            orderby (str): Variable name that instances are sorted by
            current_ins (dict[str, str]): Instance currently being validated

        Returns:
            dict[str, str]: Previous instance or False if no instance found
        """

        pass
    