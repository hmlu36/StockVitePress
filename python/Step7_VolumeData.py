from io import StringIO
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from datetime import date
import random
from functools import reduce
import os
import errno
import pyuser_agent

import re
from PIL import Image
from IPython.display import display
from pytesseract import image_to_string
import cv2


# 參考
# https://gist.github.com/CMingTseng/79447ccb2bb41e4bd8ec36d020fccab9
# https://github.com/Pregaine/stock/blob/master/01_Day%20process/%E5%88%B8%E5%95%86%E5%88%86%E9%BB%9E/%E6%8D%89%E5%8F%96%E5%8D%B7%E5%95%86%E8%B2%B7%E8%B3%A3.py
# 公式 範例
# https://blog.cnyes.com/my/uniroselee/article2270853

base_url = "https://bsr.twse.com.tw/bshtm"
path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Data", "Daily", "Chip")

# 交易日期
receive_date = ""

# 成交筆數
trade_rec = 0

# 成交金額
trade_amt = 0


def DownloadVolume(stockId):
    session = requests.Session()
    ua = pyuser_agent.UA()
    user_agent = ua.random
    headers = {"user-agent": user_agent}
    response = session.get(f"{base_url}/bsMenu.aspx", headers=headers, verify=True)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")

        # 辨識Captcha
        img_url = soup.findAll("img")[1]["src"]
        print(img_url)

        img = GetCaptcha(f"{base_url}/{img_url}")
        captcha = DecodeCaptcha(img)
        print("captcha: " + captcha)

        params = {}

        # 取得頁面上session參數資料
        nodes = soup.select("form input")
        for node in nodes:
            name = node.attrs["name"]

            # 忽略鉅額交易的 radio button
            if name in ("RadioButton_Excd", "Button_Reset"):
                continue

            if "value" in node.attrs:
                params[node.attrs["name"]] = node.attrs["value"]
            else:
                params[node.attrs["name"]] = ""

        params["CaptchaControl1"] = captcha
        params["TextBox_Stkno"] = stockId

        # 送出
        # print(json.dumps(params, indent=2))
        resp = session.post(f"{base_url}/bsMenu.aspx", data=params, headers=headers)
        if resp.status_code != 200:
            print("任務失敗: %d" % resp.status_code)
            return {"success": False}

        soup = BeautifulSoup(resp.text, "lxml")
        errorMessage = soup.select("#Label_ErrorMsg")[0].get_text()

        if errorMessage:
            print("錯誤訊息: " + errorMessage)
            return {"success": False}
        else:
            nodes = soup.select("#HyperLink_DownloadCSV")
            if len(nodes) == 0:
                print("任務失敗，沒有下載連結")
                return {"success": False}

            # 下載分點進出 CSV
            resp = session.get(f"{base_url}/bsContent.aspx",verify=True)
            if resp.status_code != 200:
                print("任務失敗，無法下載分點進出 CSV")
                return {"success": False}

            # print(resp.text)
            resp = session.get(f"{base_url}/bsContent.aspx?v=t",verify=True)
            soup = BeautifulSoup(resp.text, "html.parser")

            # 交易日期
            receive_date = soup.select_one("#receive_date").text.replace("/", "").strip()

            # 成交筆數
            trade_rec = soup.select_one("#trade_rec").text.strip()

            # 成交金額
            trade_amt = soup.select_one("#trade_amt").text.strip()

            print("receive_date:" + receive_date + ", trade_rec:" + trade_rec + ", trade_amt:" + trade_amt)

            # 重組table(取出class有column_value_price_2, column_value_price_3)
            trs = soup.find_all("tr", {"class": ["column_value_price_2", "column_value_price_3"]})
            # print(str(trs))

            soup = BeautifulSoup(f"<table>{str(trs)}</table", "html.parser")
            data = soup.select_one("table")
            df = pd.read_html(StringIO(data.prettify()))[0]
            df.columns = ["序號", "券商", "價格", "買進股數", "賣出股數"]
            df.dropna(subset=["券商"], inplace=True)  # 移除空白列
            df["買進股數"] = df["買進股數"].astype(int)
            df["賣出股數"] = df["賣出股數"].astype(int)

            # 去掉中文和空白
            df["券商"] = df["券商"].replace(regex=r"[\u4e00-\u9fa5]", value="").replace(regex=r" +$", value="")
            # print(df)

            # 寫檔案
            filePath = os.path.join(path, receive_date, f"{stockId}.csv")
            # 建立資料夾, 如果資料夾不存在時
            if not os.path.exists(os.path.dirname(filePath)):
                try:
                    os.makedirs(os.path.dirname(filePath))
                except OSError as exc:  # Guard against race condition
                    if exc.errno != errno.EEXIST:
                        raise
            df.to_csv(filePath, encoding="utf_8_sig")

            return {"success": True, "receive_date": receive_date, "trade_rec": trade_rec, "trade_amt": trade_amt}


def GetVolumeIndicator(result, stockId):
    df = pd.read_csv(os.path.join(path, result["receive_date"], f"{stockId}.csv"))

    # TOP 1 買超 = 買最多股票的券商 買多少
    top1Buy = df["買進股數"].max()

    # TOP 1 賣超 = 賣最多股票的券商 賣多少
    top1Sell = df["賣出股數"].max()
    # 超額買超 = TOP 1 買超 / TOP 1 賣超
    overBuy = round(top1Buy / top1Sell, 2)

    # 總成交量
    totalVolume = df["買進股數"].sum() / 1000

    # 重押比例 > 30%
    top1BuyPercent = (top1Buy / 1000) / totalVolume

    allInSecurities = ""
    if totalVolume > 100:  # 大於100張才計算
        if (top1BuyPercent) > 0.3:
            mainSecurities = df[df["買進股數"] == df["買進股數"].max()]["券商"].values[0]
            print("主要券商:" + mainSecurities)
            allInSecurities = mainSecurities + " (" + str(round(top1BuyPercent * 100, 3)) + "%) "

    # 買超張數 > 500, 買超異常4倍
    if totalVolume > 500:
        if overBuy > 4.0 and (top1Buy / 1000 > 500):
            overBuy = "🏆" + str(overBuy)
        elif overBuy < 0.25 and (top1Sell / 1000 > 500):
            overBuy = "⚠️" + str(overBuy)

    print("top1Buy:" + str(top1Buy) + ", top1Sell:" + str(top1Sell) + ", overBuy:" + str(overBuy))

    # 買方的前 15 名買超量
    top15Buy = df.sort_values("買進股數", ascending=False).head(15)["買進股數"].sum() / 1000
    # 賣方的前 15 名賣超量
    top15Sell = df.sort_values("賣出股數", ascending=False).head(15)["賣出股數"].sum() / 1000
    # 前15名買賣超量 = 買方的前 15 名買超量 - 賣方的前 15 名賣超量
    top15Volume = round(top15Buy - top15Sell, 3)
    print("top15Buy:" + str(top15Buy) + ", top15Sell:" + str(top15Sell) + ", top15Volume:" + str(top15Volume))

    # 前15名買賣超量集中度(%) = 前15名買賣超量 ÷ 總成交量
    top15VolumeRate = round(top15Volume / totalVolume * 100, 2)
    prefixIcon = ""

    # 前15卷商籌碼集中度 > 20%
    if totalVolume > 500:
        if top15VolumeRate > 20:
            prefixIcon = "🏆"
        elif top15VolumeRate < -10:
            prefixIcon = "⚠️"
    top15VolumeRate = prefixIcon + str(top15VolumeRate)
    print("totalVolume:" + str(totalVolume) + ", top15Volume:" + str(top15Volume) + ", top15VolumeRate:" + str(top15VolumeRate))

    # 買賣家數差 = 買進券商數 - 賣出券商數
    buySecuritiesCount = np.count_nonzero(df["買進股數"])
    sellSecuritiesCount = np.count_nonzero(df["賣出股數"])
    buySecuritiesDiff = buySecuritiesCount - sellSecuritiesCount
    print("buySecuritiesCount:" + str(buySecuritiesCount) + ", sellSecuritiesCount:" + str(sellSecuritiesCount))
    return pd.DataFrame([[overBuy, allInSecurities, top15VolumeRate, buySecuritiesDiff]], columns=["超額買超", "重押券商", "前15卷商籌碼集中度", "買賣家數差"])


def GetVolume(stockId):
    error_count = 0
    max_error_count = 10  # 最多10次
    while error_count < max_error_count:
        try:
            result = DownloadVolume(stockId)
            if result["success"]:
                return GetVolumeIndicator(result, stockId)
            else:
                time.sleep(random.randint(1, 5))
                error_count = error_count + 1
                print(f"錯誤次數{error_count}")

        except Exception as e:
            print(str(e))


# ------ 共用的 function ------
"""
ref : https://stackoverflow.com/questions/57545125/attributeerror-module-scipy-misc-has-no-attribute-toimage
https://github.com/hhschu/Captcha_OCR/blob/master/TWSE%20Captcha%20OCR%20Challenge.ipynb
"""

_errstr = "Mode is unknown or incompatible with input array shape."


def GetCaptcha(url):
    print(url)
    img = bytes()
    res = requests.get(url, verify=True)
    if res.status_code == 200:
        img = res.content
        captcha_dir = os.path.join("Data", "Temp", "Captcha")
        os.makedirs(captcha_dir, exist_ok=True)  # 確保目錄存在
        with open(os.path.join(captcha_dir, "check.png"), "wb") as handler:
            handler.write(img)
    else:
        print("error")

    return img


def bytescale(data, cmin=None, cmax=None, high=255, low=0):
    """
    Byte scales an array (image).
    Byte scaling means converting the input image to uint8 dtype and scaling
    the range to ``(low, high)`` (default 0-255).
    If the input image already has dtype uint8, no scaling is done.
    This function is only available if Python Imaging Library (PIL) is installed.
    Parameters
    ----------
    data : ndarray
        PIL image data array.
    cmin : scalar, optional
        Bias scaling of small values. Default is ``data.min()``.
    cmax : scalar, optional
        Bias scaling of large values. Default is ``data.max()``.
    high : scalar, optional
        Scale max value to `high`.  Default is 255.
    low : scalar, optional
        Scale min value to `low`.  Default is 0.
    Returns
    -------
    img_array : uint8 ndarray
        The byte-scaled array.
    Examples
    --------
    >>> from scipy.misc import bytescale
    >>> img = np.array([[ 91.06794177,   3.39058326,  84.4221549 ],
    ...                 [ 73.88003259,  80.91433048,   4.88878881],
    ...                 [ 51.53875334,  34.45808177,  27.5873488 ]])
    >>> bytescale(img)
    array([[255,   0, 236],
           [205, 225,   4],
           [140,  90,  70]], dtype=uint8)
    >>> bytescale(img, high=200, low=100)
    array([[200, 100, 192],
           [180, 188, 102],
           [155, 135, 128]], dtype=uint8)
    >>> bytescale(img, cmin=0, cmax=255)
    array([[91,  3, 84],
           [74, 81,  5],
           [52, 34, 28]], dtype=uint8)
    """
    if data.dtype == np.uint8:
        return data

    if high > 255:
        raise ValueError("`high` should be less than or equal to 255.")
    if low < 0:
        raise ValueError("`low` should be greater than or equal to 0.")
    if high < low:
        raise ValueError("`high` should be greater than or equal to `low`.")

    if cmin is None:
        cmin = data.min()
    if cmax is None:
        cmax = data.max()

    cscale = cmax - cmin
    if cscale < 0:
        raise ValueError("`cmax` should be larger than `cmin`.")
    elif cscale == 0:
        cscale = 1

    scale = float(high - low) / cscale
    bytedata = (data - cmin) * scale + low
    return (bytedata.clip(low, high) + 0.5).astype(np.uint8)


def toimage(arr, high=255, low=0, cmin=None, cmax=None, pal=None, mode=None, channel_axis=None):
    """Takes a numpy array and returns a PIL image.
    This function is only available if Python Imaging Library (PIL) is installed.
    The mode of the PIL image depends on the array shape and the `pal` and
    `mode` keywords.
    For 2-D arrays, if `pal` is a valid (N,3) byte-array giving the RGB values
    (from 0 to 255) then ``mode='P'``, otherwise ``mode='L'``, unless mode
    is given as 'F' or 'I' in which case a float and/or integer array is made.
    .. warning::
        This function uses `bytescale` under the hood to rescale images to use
        the full (0, 255) range if ``mode`` is one of ``None, 'L', 'P', 'l'``.
        It will also cast data for 2-D images to ``uint32`` for ``mode=None``
        (which is the default).
    Notes
    -----
    For 3-D arrays, the `channel_axis` argument tells which dimension of the
    array holds the channel data.
    For 3-D arrays if one of the dimensions is 3, the mode is 'RGB'
    by default or 'YCbCr' if selected.
    The numpy array must be either 2 dimensional or 3 dimensional.
    """
    data = np.asarray(arr)
    if np.iscomplexobj(data):
        raise ValueError("Cannot convert a complex-valued array.")
    shape = list(data.shape)
    valid = len(shape) == 2 or ((len(shape) == 3) and ((3 in shape) or (4 in shape)))
    if not valid:
        raise ValueError("'arr' does not have a suitable array shape for any mode.")
    if len(shape) == 2:
        shape = (shape[1], shape[0])  # columns show up first
        if mode == "F":
            data32 = data.astype(np.float32)
            image = Image.frombytes(mode, shape, data32.tostring())
            return image
        if mode in [None, "L", "P"]:
            bytedata = bytescale(data, high=high, low=low, cmin=cmin, cmax=cmax)
            image = Image.frombytes("L", shape, bytedata.tobytes())
            if pal is not None:
                image.putpalette(np.asarray(pal, dtype=np.uint8).tostring())
                # Becomes a mode='P' automagically.
            elif mode == "P":  # default gray-scale
                pal = np.arange(0, 256, 1, dtype=np.uint8)[:, np.newaxis] * np.ones((3,), dtype=np.uint8)[np.newaxis, :]
                image.putpalette(np.asarray(pal, dtype=np.uint8).tostring())
            return image
        if mode == "1":  # high input gives threshold for 1
            bytedata = data > high
            image = Image.frombytes("1", shape, bytedata.tostring())
            return image
        if cmin is None:
            cmin = np.amin(np.ravel(data))
        if cmax is None:
            cmax = np.amax(np.ravel(data))
        data = (data * 1.0 - cmin) * (high - low) / (cmax - cmin) + low
        if mode == "I":
            data32 = data.astype(np.uint32)
            image = Image.frombytes(mode, shape, data32.tostring())
        else:
            raise ValueError(_errstr)
        return image

    # if here then 3-d array with a 3 or a 4 in the shape length.
    # Check for 3 in datacube shape --- 'RGB' or 'YCbCr'
    if channel_axis is None:
        if 3 in shape:
            ca = np.flatnonzero(np.asarray(shape) == 3)[0]
        else:
            ca = np.flatnonzero(np.asarray(shape) == 4)
            if len(ca):
                ca = ca[0]
            else:
                raise ValueError("Could not find channel dimension.")
    else:
        ca = channel_axis

    numch = shape[ca]
    if numch not in [3, 4]:
        raise ValueError("Channel axis dimension is not valid.")

    bytedata = bytescale(data, high=high, low=low, cmin=cmin, cmax=cmax)
    if ca == 2:
        strdata = bytedata.tostring()
        shape = (shape[1], shape[0])
    elif ca == 1:
        strdata = np.transpose(bytedata, (0, 2, 1)).tostring()
        shape = (shape[2], shape[0])
    elif ca == 0:
        strdata = np.transpose(bytedata, (1, 2, 0)).tostring()
        shape = (shape[2], shape[1])
    if mode is None:
        if numch == 3:
            mode = "RGB"
        else:
            mode = "RGBA"

    if mode not in ["RGB", "RGBA", "YCbCr", "CMYK"]:
        raise ValueError(_errstr)

    if mode in ["RGB", "YCbCr"]:
        if numch != 3:
            raise ValueError("Invalid array shape for mode.")
    if mode in ["RGBA", "CMYK"]:
        if numch != 4:
            raise ValueError("Invalid array shape for mode.")

    # Here we know data and mode is correct
    image = Image.frombytes(mode, shape, strdata)
    return image


def DecodeCaptcha(captcha):
    # Convert the image file to a Numpy array and read it into a OpenCV file.
    captcha = np.asarray(bytearray(captcha), dtype="uint8")
    captcha = cv2.imdecode(captcha, cv2.IMREAD_GRAYSCALE)

    # Let's first see what the original image looks like.
    print("before:")
    display(toimage(captcha))

    # Convert the captcha to black and white.
    (thresh, captcha) = cv2.threshold(captcha, 128, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)

    # Erode the image to remove dot noise and that wierd line. I use a 3x3 rectengal as the kernal.
    captcha = cv2.erode(captcha, np.ones((3, 3), dtype=np.uint8))

    # Convert the image to black and white and again to further remove noise.
    (thresh, captcha) = cv2.threshold(captcha, 128, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)

    # Some cosmetic
    captcha = cv2.fastNlMeansDenoising(captcha, h=50)

    # Turn the Numpy array back into a image
    captcha = toimage(captcha)

    # Check the result of our cleaning process
    print("after:")
    display(captcha)

    return re.sub("[^0-9A-Z]+", "", image_to_string(captcha).upper())


# ------ 測試 ------

# df = GetVolume("3257")
# print(df)
