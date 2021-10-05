from aenum import Enum

class Permission(Enum):
    AccessNotGranted = 1
    AccessGranted = 2
    AccessDenied = 3
