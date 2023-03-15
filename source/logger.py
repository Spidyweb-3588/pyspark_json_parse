# logger.py
import time

def time_elapse(method):
    def timed(*args, **kwargs):
        ts = time.time()
        print("---------------------------------------")
        print(f'"{method.__name__}" started')
        result = method(*args, **kwargs)
        te = time.time()
        print(f'"{method.__name__}" finished')
        if round(te-ts,2) < 60:
            print(f'"{method.__name__}" function took {round(te-ts,2)} secs')
            print("---------------------------------------\n")
        elif round(te-ts,2) >= 60:
            print(f'"{method.__name__}" function took {round(te-ts/60,2)} mins')
            print("---------------------------------------\n")
        elif round(te-ts,2) >= 3600:
            print(f'"{method.__name__}" function took {round(te-ts/3600,2)} hours')
            print("---------------------------------------\n")
        else:
            print('something went wrong!')
            print("---------------------------------------\n")
        return result
    return timed