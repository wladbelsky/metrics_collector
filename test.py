from webbrowser import get


class custom_dict(dict):
    table = ''
    def load(self):
        if not self:
            self.update({'key': 'value'}) # results from db
    
    def __getitem__(self, __k):
        self.load()
        return super().__getitem__(__k)
    
    def get(self, __k, default=None):
        self.load()
        return super().get(__k, default)
    
    def __str__(self) -> str:
        self.load()
        return super().__str__()
    
    def __iter__(self):
        self.load()
        return super().__iter__()

    def keys(self):
        self.load()
        return super().keys()
    
    def values(self):
        self.load()
        return super().values()
    
    def items(self):
        self.load()
        return super().items()

my_dict = custom_dict()
print(my_dict)