import json
import xmltodict


class DataReader:
    """Read data from file with specified format"""

    @staticmethod
    def read_file(file_name, file_format):
        with open('{}.{}'.format(file_name, file_format), 'r') as f:
            text_file = json.load(f)
        return text_file


class DataWriter():
    """Write data to file with specified format"""

    @staticmethod
    def write_to_file(file_name, file_format, text):
        if file_format.lower() == 'xml':
            DataWriter.write_to_file_xml(file_name, file_format, text)
        else:
            DataWriter.write_to_file_json(file_name, file_format, text)

    @staticmethod
    def write_to_file_json(file_name, file_format, text):
        with open('{}.{}'.format(file_name, file_format), 'w') as f:
            json.dump(text, f)

    @staticmethod
    def write_to_file_xml(file_name, file_format, text):
        dict_text = {'root': text}
        xml_text = xmltodict.unparse(dict_text, pretty=True, full_document=False)
        with open('{}.{}'.format(file_name, file_format), 'w') as f:
            f.write(xml_text)
