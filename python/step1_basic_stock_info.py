"""
台股資料分析系統
整合多個資料來源，提供股票基本資訊分析功能
"""

import pandas as pd
from datetime import datetime, timedelta, date
from io import StringIO
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

# 假設這些是從 utils.py 匯入的函式
from utils import (
    get_dataframe_by_css_selector,
    init,
    fetch_data,
    post_data,
)


@dataclass
class StockConfig:
    """集中管理所有配置參數"""
    # API URLs
    TWSE_DAILY_REPORT_URL: str = "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d?response=json"
    TWSE_DAILY_EXCHANGE_URL: str = "https://www.twse.com.tw/rwd/zh/afterTrading/BFT41U?selectType=ALL&response=json"
    MOPS_CAPITAL_URL: str = "https://mopsfin.twse.com.tw/opendata/t187ap03_L.csv"
    MOPS_API_URL: str = "https://mops.twse.com.tw/mops/api/redirectToOld"
    TDCC_SHAREHOLDER_URL: str = "https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5"
    DIRECTOR_SHAREHOLDER_URL: str = "https://norway.twsthr.info/StockBoardTop.aspx"
    
    # 篩選條件
    PE_RATIO_THRESHOLD: float = 10.0
    YIELD_THRESHOLD: float = 3.0
    LISTING_YEARS_THRESHOLD: int = 5
    
    # 欄位配置
    DAILY_REPORT_COLUMNS: List[str] = field(default_factory=lambda: [
        "證券代號", "證券名稱", "收盤價", "殖利率(%)", 
        "股利年度", "本益比", "股價淨值比", "財報年/季"
    ])
    
    COLUMN_RENAME_MAP: Dict[str, str] = field(default_factory=lambda: {
        "殖利率(%)": "殖利率",
        "股價淨值比": "淨值比",
        "公司代號": "證券代號",
        "實收資本額": "資本額"
    })
    
    # 財務報表類型對應
    REPORT_TYPE_MAP: Dict[str, str] = field(default_factory=lambda: {
        "綜合損益": "t163sb04",
        "資產負債": "t163sb05",
        "營益分析": "t163sb06"
    })


class DataProcessor:
    """資料處理工具類別"""
    
    @staticmethod
    def ensure_string_type(df: pd.DataFrame, column: str) -> pd.DataFrame:
        """確保指定欄位為字串型別"""
        if column in df.columns:
            df[column] = df[column].astype(str)
        return df
    
    @staticmethod
    def convert_to_numeric(df: pd.DataFrame, column: str, 
                          replace_values: Optional[Dict] = None, 
                          fill_value: float = 0) -> pd.DataFrame:
        """將欄位轉換為數值型別"""
        if column not in df.columns:
            return df
            
        series = df[column].copy()
        
        if replace_values:
            for old_val, new_val in replace_values.items():
                series = series.replace(old_val, new_val)
        
        df[column] = pd.to_numeric(series, errors='coerce').fillna(fill_value)
        return df
    
    @staticmethod
    def apply_filters(df: pd.DataFrame, filters: Dict) -> pd.DataFrame:
        """套用多個篩選條件"""
        result = df.copy()
        
        for column, condition in filters.items():
            if column in result.columns:
                if isinstance(condition, dict):
                    if 'min' in condition:
                        result = result[result[column] >= condition['min']]
                    if 'max' in condition:
                        result = result[result[column] <= condition['max']]
                elif callable(condition):
                    result = result[condition(result[column])]
        
        return result


class StockDataFetcher:
    """負責從各個來源獲取股票資料"""
    
    def __init__(self, config: StockConfig):
        self.config = config
        self.processor = DataProcessor()
    
    def get_daily_exchange_report(self, apply_filter: bool = False) -> pd.DataFrame:
        """獲取每日交易報告"""
        try:
            logger.info("正在獲取每日交易報告...")
            response = fetch_data(self.config.TWSE_DAILY_REPORT_URL)
            data = response.json().get("data", [])
            
            if not data:
                logger.warning("每日交易報告無資料")
                return pd.DataFrame()
            
            df = pd.DataFrame(data, columns=self.config.DAILY_REPORT_COLUMNS)
            df = df.rename(columns=self.config.COLUMN_RENAME_MAP)
            
            if apply_filter:
                df = self._apply_exchange_filters(df)
            
            logger.info(f"成功獲取 {len(df)} 筆每日交易資料")
            return df
            
        except Exception as e:
            logger.error(f"獲取每日交易報告失敗: {e}")
            return pd.DataFrame()
    
    def _apply_exchange_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """套用交易報告篩選條件"""
        # 轉換數值欄位
        df = self.processor.convert_to_numeric(df, "本益比")
        df = self.processor.convert_to_numeric(df, "殖利率", {"-": "0"})
        df = self.processor.convert_to_numeric(df, "淨值比")
        
        # 套用篩選條件
        filters = {
            "本益比": {"max": self.config.PE_RATIO_THRESHOLD},
            "殖利率": {"min": self.config.YIELD_THRESHOLD}
        }
        
        return self.processor.apply_filters(df, filters)
    
    def get_stock_capital(self, apply_filter: bool = False) -> pd.DataFrame:
        """獲取股本資料"""
        try:
            logger.info("正在獲取股本資料...")
            response = fetch_data(self.config.MOPS_CAPITAL_URL)
            response.encoding = "utf-8"
            df = pd.read_csv(StringIO(response.text))
            
            if apply_filter:
                df = self._apply_capital_filters(df)
            
            # 處理資本額（轉換為億元）
            df["實收資本額"] = pd.to_numeric(df["實收資本額"], errors='coerce') / 100000000
            
            result_columns = ["公司代號", "公司名稱", "實收資本額", "成立日期", "上市日期"]
            df = df[result_columns].rename(columns=self.config.COLUMN_RENAME_MAP)
            
            logger.info(f"成功獲取 {len(df)} 筆股本資料")
            return df
            
        except Exception as e:
            logger.error(f"獲取股本資料失敗: {e}")
            return pd.DataFrame()
    
    def _apply_capital_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """套用股本資料篩選條件"""
        cutoff_date = datetime.today() - timedelta(days=self.config.LISTING_YEARS_THRESHOLD * 365)
        cutoff_str = cutoff_date.strftime("%Y%m%d")
        
        df["上市日期"] = pd.to_datetime(df["上市日期"], format="%Y%m%d", errors='coerce')
        return df[df["上市日期"] < cutoff_str]
    
    def get_daily_exchange(self) -> pd.DataFrame:
        """獲取盤後定價交易資料"""
        try:
            logger.info("正在獲取盤後定價交易資料...")
            response = fetch_data(self.config.TWSE_DAILY_EXCHANGE_URL)
            data = response.json()
            
            df = pd.DataFrame(data["data"], columns=data["fields"])
            result = df[["證券代號", "成交價"]]
            
            logger.info(f"成功獲取 {len(result)} 筆盤後交易資料")
            return result
            
        except Exception as e:
            logger.error(f"獲取盤後交易資料失敗: {e}")
            return pd.DataFrame()


class FinancialReportProcessor:
    """處理財務報表相關功能"""
    
    def __init__(self, config: StockConfig):
        self.config = config
    
    def get_operating_margin(self) -> pd.DataFrame:
        """獲取營業利益率資料"""
        try:
            logger.info("正在獲取營業利益率資料...")
            df = self.get_financial_statement("營益分析")
            
            if df.empty:
                return df
            
            df.columns = [
                "證券代號", "公司名稱", "營業收入", "毛利率",
                "營業利益率", "稅前純益率", "稅後純益率"
            ]
            
            df["營業收入"] = pd.to_numeric(df["營業收入"], errors='coerce') / 100
            result = df.drop(columns=["公司名稱"])
            
            logger.info(f"成功獲取 {len(result)} 筆營業利益率資料")
            return result
            
        except Exception as e:
            logger.error(f"獲取營業利益率資料失敗: {e}")
            return pd.DataFrame()
    
    def get_financial_statement(self, report_type: str = "綜合損益", 
                              year: Optional[int] = None, 
                              season: Optional[int] = None) -> pd.DataFrame:
        """獲取財務報表"""
        try:
            if year is None or season is None:
                year, season = self.get_latest_report_period()
            
            if not (1 <= season <= 4):
                raise ValueError("季度必須是 1, 2, 3, 4 之一")
            
            ajax_code = self.config.REPORT_TYPE_MAP.get(report_type)
            if not ajax_code:
                logger.error(f"不支援的報表類型: {report_type}")
                return pd.DataFrame()
            
            # 構建請求參數
            payload = {
                "apiName": f"ajax_{ajax_code}",
                "parameters": {
                    "year": str(year),
                    "season": str(season).zfill(2),
                    "TYPEK": "sii",
                    "isQuery": "Y",
                    "firstin": 1,
                    "off": 1,
                    "step": 1,
                    "encodeURIComponent": 1,
                },
            }
            
            # 發送請求
            response = post_data(self.config.MOPS_API_URL, json=payload)
            api_response = response.json()
            final_url = api_response.get("result", {}).get("url")
            
            if not final_url:
                logger.error("無法獲取財務報表 URL")
                return pd.DataFrame()
            
            # 獲取最終資料
            final_response = fetch_data(final_url)
            
            if "查詢無資料" in final_response.text:
                logger.warning(f"查詢無資料：民國 {year} 年第 {season} 季 {report_type}")
                return pd.DataFrame()
            
            # 解析 HTML 表格
            df_list = pd.read_html(StringIO(final_response.text))
            if not df_list:
                logger.warning("找不到任何表格")
                return pd.DataFrame()
            
            df = df_list[0]
            df = df[df.iloc[:, 0] != df.columns[0]]  # 移除重複標頭
            df = df.rename(columns={"公司代號": "證券代號"})
            
            # 轉換數值欄位
            for col in df.columns[2:]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            
            return df.reset_index(drop=True)
            
        except Exception as e:
            logger.error(f"獲取財務報表失敗: {e}")
            return pd.DataFrame()
    
    @staticmethod
    def get_latest_report_period(today: Optional[date] = None) -> Tuple[int, int]:
        """計算最新財報期間"""
        if today is None:
            today = date.today()
            
        year = today.year
        roc_year = year - 1911
        
        # 財報公布截止日
        deadlines = {
            1: date(year, 5, 15),   # Q1
            2: date(year, 8, 14),   # Q2  
            3: date(year, 11, 14),  # Q3
            4: date(year, 3, 31)    # Q4 (前一年)
        }
        
        if today <= deadlines[4]:
            return roc_year - 2, 3
        elif today <= deadlines[1]:
            return roc_year - 1, 4
        elif today <= deadlines[2]:
            return roc_year, 1
        elif today <= deadlines[3]:
            return roc_year, 2
        else:
            return roc_year, 3


class ShareholderDataProcessor:
    """處理股東相關資料"""
    
    def __init__(self, config: StockConfig):
        self.config = config
    
    def get_director_shareholders(self) -> pd.DataFrame:
        """獲取董監持股資料"""
        try:
            logger.info("正在獲取董監持股資料...")
            css_selector = "#details"
            df = get_dataframe_by_css_selector(
                self.config.DIRECTOR_SHAREHOLDER_URL, 
                css_selector
            )
            
            if df.empty:
                logger.warning("董監持股資料為空")
                return df
                
            df.columns = df.columns.get_level_values(0)
            df = df.iloc[:, [3][7]]
            df["證券代號"] = df["個股代號/名稱"].str[0:4]
            df = df.rename(columns={"持股比率 %": "全體董監持股(%)"})
            
            result = df[["證券代號", "全體董監持股(%)"]]
            logger.info(f"成功獲取 {len(result)} 筆董監持股資料")
            return result
            
        except Exception as e:
            logger.error(f"獲取董監持股資料失敗: {e}")
            return pd.DataFrame()
    
    def get_all_shareholder_distribution(self) -> pd.DataFrame:
        """獲取股東分布資料"""
        try:
            logger.info("正在獲取股東分布資料...")
            df = pd.read_csv(self.config.TDCC_SHAREHOLDER_URL)
            
            if df.empty:
                logger.warning("股東分布資料為空")
                return df
            
            # 處理股東分布資料
            df["key2"] = df.groupby("證券代號").cumcount() + 1
            s = (
                df.set_index(["資料日期", "證券代號", "key2"])
                .unstack()
                .sort_index(level=1, axis=1)
            )
            s.columns = s.columns.map("{0[0]}_{0[1]}".format)
            s = s.rename_axis([None], axis=1).reset_index()
            
            # 定義持股分級
            retail_headers = [
                "1-999", "1,000-5,000", "5,001-10,000", "10,001-15,000",
                "15,001-20,000", "20,001-30,000", "30,001-40,000", 
                "40,001-50,000", "50,001-100,000"
            ]
            
            distribution_range_headers = retail_headers + [
                "100,001-200,000", "200,001-400,000", "400,001-600,000",
                "600,001-800,000", "800,001-1,000,000", "1,000,001",
                "差異數調整", "合計"
            ]
            
            new_title = ["資料日期", "證券代號"] + [
                distribution + title
                for distribution in distribution_range_headers
                for title in ["人數", "比例", "持股分級", "股數"]
            ]
            s.columns = new_title
            
            # 計算各級別統計
            s["100張以下比例"] = s[
                [retail_header + "比例" for retail_header in retail_headers]
            ].sum(axis=1)
            s["100張以下人數"] = s[
                [retail_header + "人數" for retail_header in retail_headers]
            ].sum(axis=1)
            
            # 重新命名欄位
            rename_map = {
                "100,001-200,000比例": "101-200張比例",
                "100,001-200,000人數": "101-200張人數",
                "200,001-400,000比例": "201-400張比例",
                "200,001-400,000人數": "201-400張人數",
                "400,001-600,000比例": "401-600張比例",
                "400,001-600,000人數": "401-600張人數",
                "600,001-800,000比例": "601-800張比例",
                "600,001-800,000人數": "601-800張人數",
                "800,001-1,000,000比例": "801-1000張比例",
                "800,001-1,000,000人數": "801-1000張人數",
                "1,000,001比例": "1000張以上比例",
                "1,000,001人數": "1000張以上人數",
            }
            s = s.rename(columns=rename_map)
            
            # 合併401-800張
            s["401-800張人數"] = s[["401-600張人數", "601-800張人數"]].sum(axis=1)
            s["401-800張比例"] = s[["401-600張比例", "601-800張比例"]].sum(axis=1)
            
            result_columns = [
                "證券代號", "100張以下人數", "100張以下比例",
                "101-200張人數", "101-200張比例", "201-400張人數", "201-400張比例",
                "401-800張人數", "401-800張比例", "801-1000張人數", "801-1000張比例",
                "1000張以上人數", "1000張以上比例"
            ]
            
            result = s[result_columns]
            logger.info(f"成功獲取 {len(result)} 筆股東分布資料")
            return result
            
        except Exception as e:
            logger.error(f"獲取股東分布資料失敗: {e}")
            return pd.DataFrame()


class StockAnalyzer:
    """股票分析主控制器"""
    
    def __init__(self, apply_filter: bool = False):
        self.config = StockConfig()
        self.fetcher = StockDataFetcher(self.config)
        self.financial_processor = FinancialReportProcessor(self.config)
        self.shareholder_processor = ShareholderDataProcessor(self.config)
        self.data_processor = DataProcessor()
        self.apply_filter = apply_filter
    
    def get_basic_stock_info(self) -> pd.DataFrame:
        """獲取基本股票資訊"""
        try:
            logger.info("開始分析股票資料...")
            
            # 獲取基礎資料
            exchange_report = self.fetcher.get_daily_exchange_report(self.apply_filter)
            capital = self.fetcher.get_stock_capital(self.apply_filter)
            
            if exchange_report.empty or capital.empty:
                logger.warning("基礎資料獲取失敗")
                return pd.DataFrame()
            
            # 確保證券代號型別一致
            exchange_report = self.data_processor.ensure_string_type(exchange_report, "證券代號")
            capital = self.data_processor.ensure_string_type(capital, "證券代號")
            
            # 合併基礎資料
            merged_df = pd.merge(capital, exchange_report, on="證券代號", how="inner")
            logger.info(f"基礎資料合併完成，共 {len(merged_df)} 筆")
            
            # 如果需要篩選，則加入額外資料
            if self.apply_filter:
                merged_df = self._add_additional_data(merged_df)
            
            # 重新排列欄位
            if not merged_df.empty:
                cols = ["證券代號", "證券名稱"] + [
                    col for col in merged_df.columns 
                    if col not in ["證券代號", "證券名稱"]
                ]
                merged_df = merged_df[cols]
            
            logger.info(f"分析完成，最終資料筆數: {len(merged_df)}")
            return merged_df
            
        except Exception as e:
            logger.error(f"獲取基本股票資訊時發生錯誤: {e}")
            return pd.DataFrame()
    
    def _add_additional_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """添加額外資料來源"""
        result = df.copy()
        
        # 定義額外資料來源
        additional_sources = [
            ("營業利益率", self.financial_processor.get_operating_margin),
            ("盤後交易", self.fetcher.get_daily_exchange),
            ("董監持股", self.shareholder_processor.get_director_shareholders),
            ("股東分布", self.shareholder_processor.get_all_shareholder_distribution)
        ]
        
        for name, fetch_func in additional_sources:
            try:
                logger.info(f"正在處理: {name}")
                additional_df = fetch_func()
                
                if not additional_df.empty:
                    additional_df = self.data_processor.ensure_string_type(additional_df, "證券代號")
                    result = pd.merge(result, additional_df, on="證券代號", how="left")
                    logger.info(f"{name} 資料合併完成")
                else:
                    logger.warning(f"{name} 無可用資料")
                    
            except Exception as e:
                logger.error(f"處理 {name} 時發生錯誤: {e}")
        
        return result
    
    def save_to_csv(self, df: pd.DataFrame, filename: str = "basic_stock_info.csv"):
        """儲存資料到 CSV"""
        try:
            output_dir = Path("public")
            output_dir.mkdir(exist_ok=True)
            output_path = output_dir / filename
            
            df.to_csv(output_path, encoding="utf-8-sig", index=False)
            logger.info(f"資料已成功輸出至: {output_path}")
            logger.info(f"共處理 {len(df)} 筆資料")
            
        except Exception as e:
            logger.error(f"儲存檔案失敗: {e}")


def main():
    try:
        # 初始化
        init()
        
        # 建立分析器並執行分析
        analyzer = StockAnalyzer(apply_filter=True)
        result_df = analyzer.get_basic_stock_info()
        
        # 儲存結果
        if not result_df.empty:
            analyzer.save_to_csv(result_df)
            print(f"\n=== 分析完成 ===")
            print(f"符合條件的股票數量: {len(result_df)}")
            print(f"資料已儲存至: public/basic_stock_info.csv")
            
            # 顯示前5筆資料預覽
            if len(result_df) > 0:
                print(f"\n前5筆資料預覽:")
                print(result_df.head().to_string(index=False))
        else:
            print("查無符合條件的資料")
            
    except Exception as e:
        logger.error(f"程式執行失敗: {e}")
        print(f"程式執行失敗: {e}")


if __name__ == "__main__":
    main()
