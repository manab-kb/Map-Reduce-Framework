import Settings.settings as settings
from FileSystem.filehandling import *
import os
import json
from multiprocessing import *


class MapReduce(object, FileHandler):

    def __init__(self, input_dir=settings.default_input_dir, output_dir=settings.default_output_dir,
                 n_mappers=settings.default_n_mappers, n_reducers=settings.default_n_reducers,
                 clean=True):
        super.__init__(self, input_dir, output_dir)
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.n_mappers = n_mappers
        self.n_reducers = n_reducers
        self.clean = clean

    def mapper(self, key, value):
        # Find a way to pass user defined files/code here
        pass

    def reducer(self, key, values_list):
        # Find a way to pass user defined files/code here
        pass

    def run_mapper(self, index):
        input_split_file = open(settings.get_input_split_file(index), "r")
        key = input_split_file.readline()
        value = input_split_file.read()
        input_split_file.close()
        if self.clean:
            os.unlink(settings.get_input_split_file(index))
        mapper_result = self.mapper(key, value)
        for reducer_index in range(self.n_reducers):
            temp_map_file = open(settings.get_temp_map_file(index, reducer_index), "w+")
            json.dump([(key, value) for (key, value) in mapper_result if self.check_position(key, reducer_index)],
                      temp_map_file)
            temp_map_file.close()

    def run_reducer(self, index):
        key_values_map = {}
        for mapper_index in range(self.n_mappers):
            temp_map_file = open(settings.get_temp_map_file(mapper_index, index), "r")
            mapper_results = json.load(temp_map_file)
            for (key, value) in mapper_results:
                if not (key in key_values_map):
                    key_values_map[key] = []
                try:
                    key_values_map[key].append(value)
                except Exception as e:
                    print("Exception while inserting key: " + str(e))
            temp_map_file.close()
            if self.clean:
                os.unlink(settings.get_temp_map_file(mapper_index, index))
        key_value_list = []
        for key in key_values_map:
            key_value_list.append(self.reducer(key, key_values_map[key]))
        output_file = open(settings.get_output_file(index), "w+")
        json.dump(key_value_list, output_file)
        output_file.close()

    def run(self, join=False):
        map_workers = []
        rdc_workers = []
        # Insert code for running HTTP server for each mapper
        # Insert code to display all running operations
        for thread_id in range(self.n_mappers):
            p = Process(target=self.run_mapper, args=(thread_id,))
            p.start()
            map_workers.append(p)
        [t.join() for t in map_workers]
        # Insert log outputs for map completion
        # Insert code for shuffle step
        # Insert log outputs for shuffle completion
        # Insert code for running HTTP server for each reducer
        # Insert code to display all running operations
        for thread_id in range(self.n_reducers):
            p = Process(target=self.run_reducer, args=(thread_id,))
            p.start()
            map_workers.append(p)
        [t.join() for t in rdc_workers]
        # Insert log outputs for reduce completion
        if join:
            self.join_outputs()

# Create shell script to run the entire job as a command
# Use command line arguments to input files, directories etc. in the shell script
