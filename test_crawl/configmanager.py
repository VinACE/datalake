import configparser as configParser
import os


class ConfigManager:
    def __init__(self):
        self.config_parser = configParser.ConfigParser(allow_no_value=True)
        file_name = os.path.join(os.getcwd(), 'config')
        print(file_name)
        self.config_parser.read(file_name)

        '''
        with open('config.ini', 'r') as g:
            self.config_parser.readfp(g)
        print(g)

        # print(self.config_parser.getint('DEFAULT', 'NRPAGES'))
        # file_name = os.path.join(os.getcwd(), './config.ini')
        # print(file_name)
        print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$***********")
        # self.config_parser.readfp(os.path.join(os.getcwd()), 'config.ini')
        print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
        '''

    def config_sectionmap(self, section):
        contents = {}
        self.config_parser.sections()
        options = self.config_parser.options(section)

        for option in options:
            try:
                contents[option] = self.config_parser.get(section, option)

                if contents[option] == -1:
                    print('skip: %s ' % option)
            except Exception:
                print('exception on %s!' % option)
                contents[option] = None
        return contents

    def config_item(self, section, key):
        content = self.config_sectionmap(section)
        try:
            ret_val = content[key.lower()]
            return ret_val
        except Exception:
            print('No value found for key %s under section %s' % (key, section))
            return None



if __name__ == '__main__':
    cm = ConfigManager()
    '''
    cm = ConfigManager()

    section = 'DEFAULT'
    i = 'NRPAGES'
    items_cm = cm.config_item(section, i)
    print(items_cm)
    '''
    # each_section = ('DEFAULT',)
    for each_section in cm.config_sectionmap('APF_CRAWL_SITE'):
        for (each_key, each_val) in cm.config_sectionmap(each_section):
            print(each_key)
            print(each_val)
