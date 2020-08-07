import pandas as pd
import numpy as np
from utils import create_UID
import utils
import time

def format_df_cols(thenames, thepoints, thevalues=None, theuids=None):
    '''
    PARAMETRE:
    thenames: pandas.Series instance, names of points
    thepoints: pandas.Series instance with shapely Point instances inlines, points
    [thevalues]: optional, pandas.Series instance, data value of points
    [theuids]: optional, pandas.Series instance, UID of points
    '''
    def reindex_list_ifnotna(thelist):
        returnlist = []
        for x in thelist:
            if x is not None:
                returnlist.append(x.reset_index(drop=True))
            else:
                returnlist.append(None)
        return returnlist
    df = pd.concat(reindex_list_ifnotna([thenames, thepoints, thevalues, theuids]),axis=1)
    colnames = ['Name','Point']
    if thevalues is not None:
        colnames.append('R0')
    if theuids is not None:
        colnames.append('UID')
    df.columns = colnames
    return df

class MyData(object):
    def __init__(self, mydf, mygdf, cashed_metric = None, instance_name = 'MyData_default', earth_avg_C = 6371):
        '''
        PARAMETRE:
        mydf: pandas.DataFrame instance, list(mydf.columns) should be like ['Name', 'Point', 'R0', ['UID']]
        mygdf: pandas.DataFrame instance, list(mygdf.columns) should be like ['Name', 'Point', ['UID']]
        ---------------------------------------------
        '''
        self.df = self.__combine(mydf, mygdf)
        self.total_datasize = len(self.df)
        if cashed_metric is None:
            self.llp = utils.LatLongPoints(self.df.Point, self.df.Name, cashed_metric=None,instance_name=instance_name, earth_avg_C=earth_avg_C)
        else:
            self.llp = utils.LatLongPoints(self.df.Point, self.df.Name, cashed_metric=cashed_metric,instance_name=instance_name, earth_avg_C=earth_avg_C)
        self.round = 0
    
    def __combine(self, df1, df2):
        if 'UID' not in df1.columns:
            df1['UID'] = create_UID(df1.Name, df1.Point)
        if 'UID' not in df2.columns:
            df2['UID'] = create_UID(df2.Name, df2.Point)
        df1['status'] = 1
        df2['status'] = 2
        
        return pd.concat([df1,df2]).set_index('UID',drop=False)
        
    def split_df(self):
        '''
        This function is used to split the instored dataframe into "datayes" DataFrame with data, and another "datano" DataFrame without data
        ---------------------------------------------
        INTERNAL_PARAMETRE:
        self.round: the interpolation round, default as 0 which represents the original data
        ---------------------------------------------
        RETURN:
        datayes, datano
        '''
        round_mark = 'R%d' %self.round
        datayes = self.df[pd.notna(self.df[round_mark])]
        datano = self.df[pd.isna(self.df[round_mark])]
        return datayes, datano
    
    @property
    def datasize(self):
        return self.get_datasize(theround = None)
    
    def get_datasize(self, theround = None):
        '''
        This property refers to the number of notna data in the latest round
        '''
        # default return latest data size
        if theround is None:
            return self.df[self.this_round_mark].notna().sum()
        # if indicate round num and it is an integer
        elif isinstance(theround, int):
            return self.df[self.__round_mark(theround)].notna().sum()
        # else, if it is a string
        else:
            return self.df[theround].notna().sum()
    
    def __round_mark(self, theround):
        return 'R%d' %(theround)
    
    @property
    def last_round_mark(self):
        return self.__round_mark(self.round-1)
    
    @property
    def this_round_mark(self):
        return self.__round_mark(self.round)
    
    def interpolate_once(self, max_distance, min_n, max_n):
        '''
        This function is designed to update data from the latest data one round further
        ---------------------------------------------
        PARAMETRE:
        max_distance: the points with data cannot be further than max_distance km(s)
        min_n: minimum data points to calculate the estimated value
        max_n: maximum data points to calculate the estimated value
        ---------------------------------------------
        INTERNAL_PARAMETRE:
        self.round: the interpolation round, must start from 1 because 0 refers to the original data
        ---------------------------------------------
        '''
        def estimate_data(current_row, max_distance, min_n, max_n):
            # UID for current row
            UID = current_row.UID
            # get olddata if any
            olddata = self.df.loc[UID, self.last_round_mark]
            # return olddata if exists
            if pd.notna(olddata):
                return olddata

            # call metric for current UID
            current_metric = self.llp[UID]

            # slice neighbour places within max_distance
            current_metric = current_metric[(0<current_metric) & (current_metric<=max_distance)]
            # slice neighbour places that has data in self.df
            current_metric = current_metric.loc[[x for x in current_metric.index if x in self.df.index]]

            # slice neighbour with data
            withdata_mask = self.df.loc[current_metric.index,self.last_round_mark].notna()
            current_metric = current_metric[withdata_mask]

            # consider n nearest neighbours
            neighbour_num = len(current_metric)

            if neighbour_num >= min_n:
                if neighbour_num <= max_n:
                    # within requested range, current_metric remains the same unsorted
                    True
                else:
                    # redundant data, current_metric sorted and sliced
                    current_metric = current_metric[current_metric.argsort()][:max_n]
            else:
                # less than requested range, current_metric is not eligible, so set to default null pandas.Series()
                current_metric = pd.Series(dtype=float, name=UID)
            # distance metric
            distance = current_metric
            # data metric
            data = self.df.loc[current_metric.index, self.last_round_mark]
            # if data exists, calculate the estimated value
            if len(distance) > 0:
                estimated_result = (data*((1/distance)/(1/distance).sum())).sum()
            # else, estimated value is null
            else:
                estimated_result = None
            return estimated_result

        # round information
        self.round += 1

        # get all data
        lastround_data = self.df

        # split data from last round if needed
        #datayes, datano = self.split_df()

        # choose data I concern
        iconcern = lastround_data

        # update data
        newround_data = iconcern.apply(estimate_data, axis=1, args = (max_distance, min_n, max_n))
        newround_data.name = self.this_round_mark
        self.df = pd.concat([self.df,newround_data],axis=1)
        return self.df
    
    def if_newdata(self):
        this_datasize = self.get_datasize(self.this_round_mark)
        last_datasize = self.get_datasize(self.last_round_mark)
        if this_datasize == last_datasize:
            return False
        elif this_datasize == len(self.df):
            return False
        else:
            return True
    
    @property
    def latest_data(self):
        i = 0
        while True:
            i += 1
            rm = self.__round_mark(i)
            if rm in self.df.columns:
                continue
            else:
                break
        latest_round = self.__round_mark(i-1)
        return self.df[latest_round]
    
    def clean_history(self):
        i = 0
        while True:
            i += 1
            rm = self.__round_mark(i)
            if rm in self.df.columns:
                self.df.drop(rm,axis=1,inplace=True)
            else:
                break
    
    def interpolate(self, max_distance, min_n, max_n, restart = True, max_round = 50):
        '''
        This function is used to interpolate requested data for multiple rounds until dataset is full or nothing more can be done.
        =====================================
        PARAMETRE:
        max_distance: the points with data cannot be further than max_distance km(s)
        min_n: minimum data points to calculate the estimated value
        max_n: maximum data points to calculate the estimated value
        restart: wipe data in the possible former interpolation rounds and restart a new one
        max_round: max loop times to avoid infinite loop
        ---------------------------------------------
        INTERNAL_PARAMETRE:
        self.round: the interpolation round, must start from 1 because 0 refers to the original data
        ---------------------------------------------
        '''
        print('Interpolation start: max_distance: %s; min_n: %d, max_n: %d, restart: %s, max_round: %d' %(str(max_distance),min_n,max_n,str(restart),max_round))
        bt = time.time()
        if restart:
            self.round = 0
            self.clean_history()
        try_i = 0
        while True:
            try_i += 1
            self.interpolate_once(max_distance, min_n, max_n)
            print('Interpolating round: %d, data point ratio: %d/%d (%.2f%%)' %(self.round, self.datasize, self.total_datasize, self.datasize/self.total_datasize*100), end='\r')
            if (not self.if_newdata()) or (try_i > max_round):
                break
        cost_time = time.time()-bt
        print('\nInterpolation completed, cost time: %d s in %d round(s)\n===============' %(cost_time, self.round))
        return self.latest_data, cost_time

    def plot(self):
        pass