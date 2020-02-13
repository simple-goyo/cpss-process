from datetime import datetime, timedelta
import time


def get_time():
    # 本地服务器
    rigth_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # utc服务器
    # now_time = datetime.now()
    # utc_time = now_time + timedelta(hours=8)  # UTC只是比北京时间提前了8个小时
    # rigth_time = utc_time.strftime("%Y-%m-%d %H:%M:%S")
    return rigth_time
