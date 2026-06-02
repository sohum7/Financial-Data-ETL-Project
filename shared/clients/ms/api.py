# Market Stack API configuration

# Built-in imports
from dataclasses import dataclass
from datetime import datetime


# Market Stack API dataclass
@dataclass(slots=True, frozen=True)
class APIConfig:
    data_type: str
    base_url: str
    symbols: list[str]
    api_key: str
    start_dt: str
    end_dt: str
    batch_dt: str
    
    def __post_init__(self):
        # Instance variable type checking
        self.type_check_vars()
        
        # Instance variable cleaning
        self.clean_vars()
    
    # Type checks for all attributes
    def type_check_vars(self):
        fields = [
            ("data_type", self.data_type, str),
            ("base_url", self.base_url, str),
            ("symbols",  self.symbols,  list), # list of strings, but we will check the individual elements later
            ("api_key",  self.api_key,  str),
            ("start_dt", self.start_dt, str),
            ("end_dt",   self.end_dt,   str),
            ("batch_dt", self.batch_dt, str)
        ]
        
        for name, value, type in fields:
            if not isinstance(value, type):
                raise TypeError(f"{name} must be {type}")
        
        if not all(isinstance(symbol, str) for symbol in self.symbols):
            raise TypeError("all symbols must be str")
    
    # Data normalization and validation
    def clean_vars(self):
        # Fields as strings and what they will be turned into post cleaning
        fields = [
            ("data_type", self.data_type.lower().strip()),
            ("base_url", self.base_url.rstrip("/")),
            ("symbols",  sorted(symbol.upper().strip() for symbol in self.symbols)),
            ("start_dt", APIConfig.normalize_date(self.start_dt)),
            ("end_dt",   APIConfig.normalize_date(self.end_dt)),
            ("batch_dt", APIConfig.normalize_date(self.batch_dt))
        ]
        # TODO: ensure start date is before end date and same for batch with respect to end date
        if not self.start_dt < self.end_dt < self.batch_dt:
            pass
        
        # Set the fields to their new values
        # Note: for frozen dataclasses this is the only way to modify variables
        for name, new_val in fields:
            object.__setattr__(self, name, new_val)
    
    # Normalize dates to proper format
    @staticmethod
    def normalize_date(date_str: str) -> str:
        date_str = date_str.strip()
        
        formats = [
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%Y/%m/%d",
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        raise ValueError(f"Invalid date format: {date_str}")
    
    # Create url w/o params
    @staticmethod
    def url_constructor(base_url: str, data_type: str) -> str:
        return f"{base_url.rstrip('/')}/{data_type.lower()}"
    
    # Derived url
    @property
    def url(self) -> str:
        return __class__.url_constructor(self.base_url, self.data_type)
    
    # Derived symbols list as string
    @property
    def symbols_str(self) -> str:
        return ",".join(self.symbols)
    
    # Derived hash input
    @property
    def hash_input(self) -> str:
        return f"{self.data_type}|{self.base_url}|{self.symbols_str}|{self.start_dt}|{self.end_dt}"
    
    # # Derived hash output
    @property
    def hash_val(self) -> int:
        return hash(self.hash_input)
    
    # User representation of APIConfig object
    def __str__(self) -> str:
        return f"data type: {self.data_type}\nbase url: {self.base_url}\nsymbols: {self.symbols}\nstart date: {self.start_dt}\nend date: {self.end_dt}\nbatch date: {self.batch_dt}\nhash value: {self.hash_val}"
    
    # Developer representation of APIConfig object
    def __repr__(self) -> str:
        return f"APIConfig(data_type={self.data_type}, base_url={self.base_url}, start_dt={self.start_dt}, end_dt={self.end_dt}, batch_dt={self.batch_dt}, hash_input={self.hash_input}, hash_val={self.hash_val}), symbols_str={self.symbols_str}"


