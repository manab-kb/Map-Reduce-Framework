import os
import json
import settings

class FileHandler(object):

    def __init__(self, input_file_path, output_dir):
        self.input_file_path = input_file_path
        self.output_dir = output_dir

    def begin_file_split(self, split_index, index):
        file_split = open(settings.get_input_split_file(split_index-1), "w+")
        file_split.write(str(index) + "\n")
        return file_split

    def is_on_split_position(self, character, index, split_size, current_split):
        return index>split_size*current_split+1 and character.isspace()

    def split_file(self, number_of_splits):
        file_size = os.path.getsize(self.input_file_path)
        unit_size = file_size / number_of_splits + 1
        original_file = open(self.input_file_path, "r")
        file_content = original_file.read()
        original_file.close()
        (index, current_split_index) = (1, 1)
        current_split_unit = self.begin_file_split(current_split_index, index)
        for character in file_content:
            current_split_unit.write(character)
            if self.is_on_split_position(character, index, unit_size, current_split_index):
                current_split_unit.close()
                current_split_index += 1
                current_split_unit = self.begin_file_split(current_split_index,index)
            index += 1
        current_split_unit.close()
