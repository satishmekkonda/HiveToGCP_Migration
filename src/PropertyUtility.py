import configparser
from configparser import RawConfigParser
class PropertyUtilty:
    
    #parser=configparser.ConfigParser()
    #conf=Constants.variables()
    def setValue(self,CONFIG_SECTION,Option,value):
        parser=RawConfigParser()
        parser.read(f'../config/properties.ini')
        parser.set(CONFIG_SECTION,Option,value)

    def getValue(self,CONFIG_SECTION,Option):
        parser=configparser.ConfigParser()
        #conf=Constants.variables()
        parser.read(f'../config/properties.ini')
        value=parser.get(CONFIG_SECTION,Option)
        return value
