import sys, os
import json
from typing import List


class BaseConverter:
    """
    That's an interface for all converters.
    It shows that any converter takes source and destination files names
    and implements 'convert' method. The particular conversion implementation
    may differ, but the usage of any child will be the same.
    """
    def __init__(self, source_file_name, dest_file_name):
        self.source_file = open(source_file_name, 'r')
        self.dest_file = open(dest_file_name, 'w')

    def convert(self):
        self._convert_data()
        self._close_files()

    def _convert_data(self):
        raise NotImplementedError

    def _close_files(self):
        self.source_file.close()
        self.dest_file.close()


Line = List[str]


class CsvToJsonConverter(BaseConverter):
    json_file_header = '{"data": ['
    json_file_footer = ']}'

    def _convert_data(self):
        self._write_json_file_header()

        headers = self.get_csv_headers()

        # we read the file line by line because processing
        # of a large file may consume too much memory
        for line in self.source_file_lines():
            parsed_line = self.parse_line(line)
            self.write_json_data(headers, parsed_line)

        self._write_json_file_footer()

    def parse_line(self, line: str) -> Line:
        parsed_line = []
        start_index = 0
        quotes_number = 0

        if line.endswith('\n'):
            line = line[:-1]

        for index, char in enumerate(line):
            # we check if the comma isn't between the quotes
            # and so is a separator, not a regular char
            if char == ',' and quotes_number % 2 == 0:
                value = line[start_index:index]

                value = self.normalize_value(value)

                parsed_line.append(value)
                start_index = index + 1

            elif char == '"':
                quotes_number += 1

        last_value = line[start_index:]
        last_value = self.normalize_value(last_value)
        parsed_line.append(last_value)

        return parsed_line

    @staticmethod
    def normalize_value(value: str) -> str:
        # remove whitespaces
        value = value.strip()

        # remove quotes
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]

        # unescape inner quotes
        value = value.replace('""', '"')

        return value

    def get_csv_headers(self) -> Line:
        return self.parse_line(
            self.source_file.readline()
        )

    def source_file_lines(self) -> str:
        while True:
            line = self.source_file.readline()

            if not line:
                break

            yield line

    def write_json_data(self, headers: Line, line: Line):
        data = dict(zip(headers, line))
        data = json.dumps(data) + ', '
        self.dest_file.write(data)

    def _write_json_file_header(self):
        self.dest_file.write(self.json_file_header)

    def _write_json_file_footer(self):
        # removing trailing comma in the end of file
        self.dest_file.seek(0, os.SEEK_END)
        self.dest_file.seek(self.dest_file.tell() - 2, os.SEEK_SET)

        # writing footer
        self.dest_file.write(self.json_file_footer)


if __name__ == '__main__':
    try:
        csv_json_converter = CsvToJsonConverter(
            sys.argv[1], sys.argv[2]
        )
        csv_json_converter.convert()
    except FileNotFoundError:
        print('Source file not found!')
    else:
        print('Data successfully converted.')
