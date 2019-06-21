from __future__ import print_function
import os


import numpy as np
import pandas as pd
import re
import json
import os
import errno
import codecs

class DataView(object):

    def __init__(self,name='default', jq_api=None,start_date=0,end_date=0, universe=None, fields=None, factors = None):

        self.name = name
        self.pre_data_ = pd.DataFrame()

        self.data_api = jq_api
        self._factor_df = pd.DataFrame()
        # self._import_factors = {}

        self.start_date = start_date
        self.end_date = end_date

        self.universe = universe
        self.fields = fields
        # self.freq = 1
        # self.all_price = True
        # self._snapshot = None
        self.factors = factors
        self.load_factors = {}

        self.labels = []
        self.load_labels = {}

        self.meta_data_list = ['start_date', 'end_date',
                               # 'name',
                               'fields', 'universe',

                               # 'pre_data_',

                               'factors',
                               'labels'
                               ]
        self.test_data_list = ['start_date', 'end_date',
                               # 'name',
                               'fields', 'universe',

                               # 'pre_data_',

                               'factors',
                               'labels',
                               
                               'pre_data_',
                               'load_factors'
                               ]
        #
        # self.index_weights = {}
        # self.industry_groups = {}

    def _data_preprocessing(self):
        api = self.data_api
        if self.universe is not None:
            self.pre_data_ = api.get_prices(securities=self.universe,start_date=self.start_date, end_date=self.end_date,fields=self.fields)
            self.pre_data_ = self._predata_reformat()
        else:
            print('Universe Error')

        self.factors = api.is_predefined_factors(factors=self.factors)
        if len(self.factors):
            self.load_factors = api.get_factors(self.universe,self.factors,
                                                self.start_date,self.end_date
                                                )
            self.load_factors = self._factor_reformat()
        else:
            print('No vaild factors.')

    def _predata_reformat(self):
        pre_data_ = self.pre_data_
        pre_data_ = pre_data_.to_frame()

        pre_data_.index = pre_data_.index.rename(['date','asset'])

        return pre_data_

    def _factor_reformat(self):
        example = self.load_factors.get(self.factors[0])
        indexs = example.stack().index.rename(['date','asset'])
        y = pd.DataFrame(index=indexs)
        # print(y)
        for factor in self.load_factors:
            y[factor] = self.load_factors.get(factor).stack()
        return y

    def save_dataview(self,save_path):
        # print ('Saving preprocessed data...')
        abs_folder = os.path.abspath(save_path)
        meta_path = os.path.join(save_path, 'METADATA_{}.json'.format(self.name))
        data_path = os.path.join(save_path, 'DATA_{}.hd5'.format(self.name))

        data_to_store = {
                        'pre_data_': self.pre_data_,
                        'load_factors':self.load_factors
                         }
        # for symbol in self.index_weights.keys():
        #     data_to_store['index_weight/' + symbol] = self.index_weights[symbol]
        #
        # for group in self.industry_groups.keys():
        #     data_to_store['industry_group/' + group] = self.industry_groups[group]
        data_to_store = {k: v for k, v in data_to_store.items() if v is not None}
        meta_data_to_store = {key: self.__dict__[key] for key in self.meta_data_list}
        # print(meta_data_to_store)

        print("\nStore data...")
        DataView._save_json(meta_data_to_store, meta_path)
        DataView._save_h5(data_path, data_to_store)

        print("Dataview has been successfully saved to:\n"
              + abs_folder + "\n\n"
              + "You can load it with load_dataview('{:s}')".format(abs_folder))

    @staticmethod
    def _save_h5(fp,dic):
        """
        Save data in dic to a hd5 file.

        Parameters
        ----------
        fp : str
            File path.
        dic : dict

        """
        import warnings
        warnings.filterwarnings('ignore', category=pd.io.pytables.PerformanceWarning)

        DataView.create_dir(fp)
        h5 = pd.HDFStore(fp, mode='w',complevel=9, complib='blosc')
        for key, value in dic.items():
            h5[key] = value
        h5.close()

    @staticmethod
    def _save_json(serializable, file_name):
        """
        Save an serializable object to JSON file.

        Parameters
        ----------
        serializable : object
        file_name : str

        """
        fn = os.path.abspath(file_name)
        DataView.create_dir(fn)

        with codecs.open(fn, 'w', encoding='utf-8') as f:
            json.dump(serializable, f, separators=(',\n', ': '))

    @staticmethod
    def create_dir(filename):
        """
        Create dir if directory of filename does not exist.

        Parameters
        ----------
        filename : str

        """
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
                else:
                    print("I dont know why but it doesn't work.")

    def load_dataview(self,load_path,name):

        path_meta_data = os.path.join(load_path, 'METADATA_{}.json'.format(name))
        path_data = os.path.join(load_path, 'DATA_{}.hd5'.format(name))

        if not ( os.path.exists(path_data)and os.path.exists(path_meta_data)):
            raise IOError("There is no data file under directory {}".format(load_path))

        meta_data = DataView.read_json(path_meta_data)
        dic = DataView._load_h5(path_data)
        self.data_d = dic.get('/data_d', None)
        self.data_q = dic.get('/data_q', None)
        self._data_benchmark = dic.get('/data_benchmark', None)
        self._data_inst = dic.get('/data_inst', None)
        self._factor_df = dic.get('/factor_df', None)
        self.pre_data_ = dic.get('/pre_data_',None)
        self.load_factors = dic.get('/load_factors',None)

        # for k in dic.keys():
        #     if k.startswith('/index_weight/'):
        #         self.index_weights[k.split('/')[2]] = dic[k]
        #     if k.startswith('/industry_group/'):
        #         self.industry_groups[k.split('/')[2]] = dic[k]

        self.__dict__.update(meta_data)

        # for index, row in self._factor_df.iterrows():
        #     factor_id = row['factor_id']
        #     factor_body = row['factor_def']
        #     factor_args = list(filter(None, row['factor_args'].split(',')))
        #     if 'factor_quarterly' in row:
        #         factor_quarterly = row['factor_quarterly']
        #     else:
        #         factor_quarterly = False

            # self._import_factors[factor_id] = FactorDef(factor_id, factor_args, factor_body, factor_quarterly)

        # self._process_data(large_memory)

        print("Dataview loaded successfully.")

    @staticmethod
    def read_json(fp):
        """
        Read JSON file to dict. Return None if file not found.

        Parameters
        ----------
        fp : str
            Path of the JSON file.

        Returns
        -------
        dict

        """
        content = dict()
        try:
            with codecs.open(fp, 'r', encoding='utf-8') as f:
                content = json.load(f)
        except IOError as e:
            if e.errno not in (errno.ENOENT, errno.EISDIR, errno.EINVAL):
                raise
        return content

    @staticmethod
    def _load_h5(fp):
        """Load data and meta_data from hd5 file.

        Parameters
        ----------
        fp : str, optional
            File path of pre-stored hd5 file.

        """
        h5 = pd.HDFStore(fp)

        res = dict()
        for key in h5.keys():
            res[key] = h5.get(key)

        h5.close()

        return res