from sklearn.metrics.pairwise import haversine_distances
from math import radians
import pandas as pd
import numpy as np
import hashlib
import shapely

def haversine_distance(p1, p2, earth_avg_C = 6371):
    '''
    This function is to calculate the Haversin distance between p1 and p2, given the earth_avg_C as the global average circumference.
    ------------------------------------------------------
    PARAMETRE:
    p1: [longtitude, latitude] or (longtitude, latitude) or shapely.geometry.point.Point
    p2: [longtitude, latitude] or (longtitude, latitude) or shapely.geometry.point.Point
    *note: longtitude must be in the first element and lattitude to be in the second
    earth_avg_C: the parametre to indicate circumference of sphere; the default 6371 means the average circumference of the earth is 6371kms
    ------------------------------------------------------
    RETURN:
    A float object indicating the haversine distance from p1 to p2
    '''
    if isinstance(p1, shapely.geometry.point.Point):
        p1 = [p1.y, p1.x]
    if isinstance(p2, shapely.geometry.point.Point):
        p2 = [p2.y, p2.x]
    p1_in_radians = [radians(x) for x in p1]
    p2_in_radians = [radians(x) for x in p2]
    result = haversine_distances([p1_in_radians, p2_in_radians]) * earth_avg_C
    return np.sum(result)/2

def create_UID(thenames, thepoints):
    '''
    This function is to create UID for each name and each point, by reproducible MD5 algorithm.
    ------------------------------------------------------
    PARAMETRE:
    thenames: a pandas Series instance with name information
    thepoints: a pandas Series instance with shapely Point instances inlines.
    ------------------------------------------------------
    RETURN:
    A pandas Series with UID information for each name and each point
    '''
    UID = (thenames.apply(str)+thepoints.apply(str)).map(lambda x: hashlib.md5(str(x).encode('utf-8')).hexdigest())
    return UID

class LatLongPoints(object):
    def __init__(self, point_coords, point_names, cashed_metric = None, instance_name='LatLongPoints_default', earth_avg_C = 6371):
        self.instance_name = instance_name
        self.earth_avg_C = earth_avg_C
        df1 = pd.DataFrame(point_coords)
        df2 = pd.DataFrame(point_names)
        _gdf = pd.concat([df1,df2],axis=1)
        _gdf.columns = ['Point','Name']
        _gdf['UID'] = create_UID(_gdf.Name, _gdf.Point)
        self.gdf = _gdf
        self.gdf.index = self.gdf.UID
        self.uid_name_dict = dict(zip(_gdf.UID,_gdf.Name))
        print('LatLongPoints: "%s" loaded\nLength: %d, earth_avg_C: %.3fkms\n%s' %(self.instance_name,len(_gdf),self.earth_avg_C,"="*40))
        if cashed_metric is None:
            self.metric = None
        else:
            self.metric = self.__consistence(cashed_metric)
    
    def __consistence(self, cashed_metric):
        return self.update_metric(cashed_metric)
        
    
    def __getitem__(self, key):
        '''
        This function only accept .iloc and .loc function with fixed value, ":" expression is not supported
        -----------------------------
        key: x, [y], can only be the same type of expression, such as [1,3] / ['UID1','UID2']
        '''
        if isinstance(key, tuple):
            x = key[0]
            y= key[1]
        elif isinstance(key, int) or isinstance(key, str):
            x = key
            y = None
        else:
            raise ValueError('Passed index is neither [int], [str], [int,int] nor [str,str]')
        if y is not None:
            if isinstance(x, int) and isinstance(y, int):
                return self.mymetric.iloc[x,y]
            elif isinstance(x, str) and isinstance(y, str):
                return self.mymetric.loc[x,y]
        else:
            if isinstance(x, int):
                return self.mymetric.iloc[x,:]
            else:
                return self.mymetric.loc[x,:]
        
    def to_csv(self):
        pass

    def read_csv(self):
        pass
    
    @property
    def mymetric(self):
        '''
        A property to return mymetric
        '''
        return self.__distance_metric()
    
    def __distance_metric(self):
        '''
        This function is to return mymetric
        '''
        if self.metric is None:
            return self.reload_metric()
        else:
            return self.metric
    
    def update_metric(self, cashed_metric):
        '''
        This function is used to auto-detect whether all points in self.gdf are cashed in cashed_metric, if not, then update the metric
        '''
        print('Detecting whether all points are cashed in the current metric: ')
        # test if cashed_metric covers all points in cashed_metric
        try:
            cashed_metric.loc[self.gdf.index]
            # if yes, slicing is successful, return cashed_metric
            print('Yes, all points in\n========================================')
            return cashed_metric
        except:
            # otherwise, detect points not in
            uncashed_points = [x for x in self.gdf.index if x not in cashed_metric.index]
            print('No, updating new points. Update size: %d' %len(uncashed_points))
            uncashed_gdf = self.gdf.loc[uncashed_points]
            newmetric = self.__refresh_metric(uncashed_gdf, self.gdf)
            updated_metric = pd.concat([cashed_metric, newmetric])
            print('Updating complete\n========================================')
            return updated_metric
    
    def reload_metric(self):
        self.metric = self.__refresh_metric(self.gdf, self.gdf)
        return self.metric
    
    def __refresh_metric(self, thegdf1, thegdf2):
        '''
        This function is the core function to refresh the metric. A metric in this object is used to log Haversin distance between each pair of Points in this object. It is usually one-off for each stable instance; once the metric is created, there is no need to refresh the metric as long as the involved Points remains the same. 
        ------------------------------------------------------
        PARAMETRE:
        n/a
        ------------------------------------------------------
        RETURN:
        pandas.DataFrame instance with distances in metric
        '''
        if len(thegdf1) > len(thegdf2):
            longgdf = thegdf1
            shortgdf = thegdf2
        else:
            longgdf = thegdf2
            shortgdf = thegdf1
        maxnum1 = len(longgdf.Point)
        __index1 = longgdf.index
        maxnum2 = len(shortgdf.Point)
        __index2 = shortgdf.index
        __allindex =  list(set(list(__index1) + list(__index2)))
        metric = np.empty((maxnum2,maxnum1))
        metric[:] = np.nan
        metric = pd.DataFrame(metric)
        metric.index = __index2
        metric.columns = __index1
        for i in range(maxnum1):
            print('Metric refreshing progress: %d/%d (%.2f%%)' %(i,maxnum1,i/maxnum1*100),end='\r')
            str1 = __index1[i]
            p1 = longgdf.Point[str1]
            for j in range(maxnum2):
                str2 = __index2[j]
                # optimization judgement
                if pd.notna(metric.loc[str2,str1]):
                    metric.loc[str1,str2] = metric.loc[str2,str1]
                    continue
                p2 = shortgdf.Point[str2]
                #simple plane distance
                #distance = p1.distance(p2)

                #haversine distance on earth
                distance = haversine_distance(p1,p2,self.earth_avg_C)
                metric.loc[str2,str1] = distance
        print('Metric refreshing progress: %d/%d (%.2f%%)' %(maxnum1,maxnum1,100),end='\r')
        return metric
    
    def nearest_n(self, k, topn):
        '''
        This function is to return "topn" Points and their information based on density. The ranking criteria is the average haversine distance from one point to its nearest "k" neighbours; the "topn" points will be sliced and returned later.
        ------------------------------------------------------
        PARAMETRE:
        k: number k of nearest neighbour points to calculate the average distance
        topn: return a DataFrame object with topn rows
        ------------------------------------------------------
        RETURN:
        return full DataFrame instance with all topn points inline
        ------------------------------------------------------
        '''
        def mean_topn(x, k):
            x = x.reset_index(drop=True)
            thesum = x[np.argsort(x)[:k+1]].sum()
            return thesum/topn
        mymetric = self.__distance_metric()
        self.gdf['mean_nearestn'] = mymetric.apply(mean_topn,axis=1,k=k)
        returnme = self.gdf.iloc[self.gdf.mean_nearestn[np.argsort(self.gdf.mean_nearestn)][:topn].index,:]
        return returnme