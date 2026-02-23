import abc



class EventBusAdapter(abc.ABC):
    """Mixin/base providing event_bus storage & setup."""

    def __init__(self, **kwargs):
        self.event_bus = None

        super().__init__(**kwargs)

    def setup_event_bus(self, event_bus):  # pragma: no cover - trivial
        self.event_bus = event_bus


class AbstractNotifications(abc.ABC):
    """
    Port (abstract interface) for notification services.
    """

    @abc.abstractmethod
    def send(self, destination, message):
        raise NotImplementedError
