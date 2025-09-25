import xlwings as xw
import pandas as pd
from dbfread2 import DBF
from pathlib import Path



def gen_path(base_dir, term,file_name):
    path= Path(base_dir) / term 
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
    
    path = path / file_name
    return path

def read_dbf(file_path:Path):
    if not file_path.exists():
        return None
    
    table = DBF(file_path,lowercase_names=True)
    df = pd.DataFrame(iter(table))
    data = df.loc[df['re']==1,
                ["补贴更正",
                    "误餐补贴",
                    "补发补贴",
                    "扣款_补贴",
                    "补发_其它",
                    "其它扣款",
                    "姓名",
                    "身份证",
                    "x_银行帐号",
                    "发放银行",
                    "银行帐号",
                    "收款行行号",
                    "发放地点",
                    "实发补贴",
                    "应发补贴"]]
    data["补贴更正"] = pd.to_numeric(data["补贴更正"], errors="coerce").fillna(0)
    data["误餐补贴"] = pd.to_numeric(data["误餐补贴"], errors="coerce").fillna(0)
    data["补发补贴"] = pd.to_numeric(data["补发补贴"], errors="coerce").fillna(0)
    data["扣款_补贴"] = pd.to_numeric(data["扣款_补贴"], errors="coerce").fillna(0)
    data["补发_其它"] = pd.to_numeric(data["补发_其它"], errors="coerce").fillna(0)
    data["其它扣款"] = pd.to_numeric(data["其它扣款"], errors="coerce").fillna(0)
    data["实发补贴"] = pd.to_numeric(data["实发补贴"], errors="coerce").fillna(0)
    data["应发补贴"] = pd.to_numeric(data["应发补贴"], errors="coerce").fillna(0)
    return data

def add_index(df:pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return None
    df = df.reset_index(drop=True)
    df.index = df.index + 1
    df = df.reset_index()
    return df



def conv_cbc(df):
    if df is None:
        return None
    
    df_ = df.loc[df["发放银行"]=="建设银行"].copy()
    if not df_.query("提高待遇>0").empty:
        result = pd.concat([
            df_.loc[df_["企业补贴"]>0,["x_银行帐号", "姓名", "企业补贴"]].copy(),
            df_.loc[
                df_["提高待遇"]>0,["x_银行帐号", "姓名", "提高待遇"]].copy()
                .rename(columns={"提高待遇":"企业补贴"})
        ])
    else:
        result = df_.loc[df_["企业补贴"]>0,["x_银行帐号", "姓名", "企业补贴"]].copy()
    if result.empty:
        return None
    return add_index(result).values.tolist()

def conv_bocny(df):
    if df is None:
        return None
    df_ = df.loc[df["发放银行"]=="中国银行_南阳"].copy()
    if not df_.query("提高待遇>0").empty:
        result = pd.concat([
            df_.loc[df_["企业补贴"]>0,["姓名", "身份证", "发放银行", "x_银行帐号", "企业补贴"]].copy(),
            df_.loc[
                df_["提高待遇"]>0,["姓名", "身份证", "发放银行", "x_银行帐号"]].copy()
                .rename(columns={"提高待遇":"企业补贴"})
        ])
    else:
        result = df_.loc[df_["企业补贴"]>0,["姓名", "身份证", "发放银行", "x_银行帐号", "企业补贴"]].copy()

    if result.empty:
        return None
    return add_index(result).values.tolist()

def conv_bocyt(df):
    if df is None:
        return None
    
    df_ = df.loc[df["发放银行"]=="中国银行_油区"].copy()
    df_["开户行"] = "中国银行"
    df_["行号"] = "41"

    if not df_.query("提高待遇>0").empty:
        result = pd.concat([
            df_.loc[df_["企业补贴"]>0,["企业补贴", "姓名", "x_银行帐号", "开户行", "行号"]].copy(),
            df_.loc[
                df_["提高待遇"]>0,["提高待遇", "姓名", "x_银行帐号", "开户行", "行号"]].copy()
                .rename(columns={"提高待遇":"企业补贴"})
        ])
    else:
        result = df_.loc[df_["企业补贴"]>0,["企业补贴", "姓名", "x_银行帐号", "开户行", "行号"]].copy()
    if result.empty:
        return None
    return result.values.tolist()


def conv_icbc(df):
    if df is None:
        return None
    # 跨行                  
    df_1 = df.loc[df["发放银行"]=="工商银行_跨行"].copy() 
    df_1["行别"] = 1
    df_1["业务种类"] = "00602"
    df_1["协议书号"] = ""
    df_1["账号地址"] = df_1["发放地点"]
    df_1["跨行行号"] = df_1["银行帐号"]

    if not df_1.query("提高待遇 > 0").empty:
        result_1 = pd.concat([
            df_1.loc[df_1["企业补贴"]>0,["姓名","x_银行帐号","行别","跨行行号","业务种类","协议书号","账号地址","企业补贴"]].copy(),
            df_1.loc[
                df_1["提高待遇"]>0,["姓名","x_银行帐号","行别","跨行行号","业务种类","协议书号","账号地址","提高待遇"]].copy()
                .rename(columns={"提高待遇":"企业补贴"})
        ])
    else:
        result_1 = df_1.loc[df_1["企业补贴"]>0,["姓名","x_银行帐号","行别","跨行行号","业务种类","协议书号","账号地址","企业补贴"]].copy()

    # 本行        
    df_2 = df.loc[df["发放银行"]=="工商银行"].copy() 
    df_2["行别"] = ""
    df_2["业务种类"] = ""
    df_2["协议书号"] = ""
    df_2["账号地址"] = ""
    df_2["跨行行号"] = ""
            
    if not df_2.query("提高待遇 > 0").empty:
        result_2 = pd.concat([
            df_2.loc[df_2["企业补贴"]>0,["姓名","x_银行帐号","行别","跨行行号","业务种类","协议书号","账号地址","企业补贴"]].copy(),
            df_2.loc[
                df_2["提高待遇"]>0,["姓名","x_银行帐号","行别","跨行行号","业务种类","协议书号","账号地址","提高待遇"]].copy()
                .rename(columns={"提高待遇":"企业补贴"})
        ])
    else:
        result_2 = df_2.loc[df_2["企业补贴"]>0,["姓名","x_银行帐号","行别","跨行行号","业务种类","协议书号","账号地址","企业补贴"]].copy()
    # 合并        
    result = pd.concat([result_1,result_2],ignore_index=True)
    if result.empty:
        return None
    return result.values.tolist()


def preprocess(df,benifits_type):
    if  df is None:
        return None
        
    match benifits_type:
        case "老人企业补贴":
            df["企业补贴"] = df["补贴更正"] + df["误餐补贴"] + df["补发补贴"] - df["扣款_补贴"]
            df["提高待遇"] = df["补发_其它"] - df["其它扣款"]
            df["企业补贴"] = pd.to_numeric(df["企业补贴"], errors="coerce").fillna(0)
            df["提高待遇"] = pd.to_numeric(df["提高待遇"], errors="coerce").fillna(0)
            df.loc[df['发放银行']=='工行异地', '发放银行'] = '工商银行_跨行'
            df["账号地址"] = df["发放地点"]
            df["跨行行号"] = df["银行帐号"]
            return df

        case "集体工企业补贴":
            df["企业补贴"] = df["补贴更正"] + df["误餐补贴"] + df["补发补贴"] - df["扣款_补贴"]
            df["提高待遇"] = df["补发_其它"] - df["其它扣款"]
            df["企业补贴"] = pd.to_numeric(df["企业补贴"], errors="coerce").fillna(0)
            df["提高待遇"] = pd.to_numeric(df["提高待遇"], errors="coerce").fillna(0)
            df.loc[(df['发放银行']=='工商银行异地') | (df['发放银行'] == '商业银行（工行代发）'), '发放银行'] = '工商银行_跨行'
            df["账号地址"] = df["发放地点"]
            df["跨行行号"] = df["银行帐号"]
            return df

        case "中人提高待遇":
            df['提高待遇'] = df["补贴更正"] + df["补发补贴"] - df["其它扣款"]
            df['企业补贴'] = 0
            df["提高待遇"] = pd.to_numeric(df["提高待遇"], errors="coerce").fillna(0)
            df.loc[(df['发放银行']=='工商银行（异地）') & (df['发放银行'] == '交通银行'), '发放银行'] = '工商银行_跨行'
            df["账号地址"] = df["发放地点"]
            df["跨行行号"] = df["收款行行号"]
            return df
        
        case _:
            pass




def export_data(templates):
    app = xw.App(visible=False, add_book=False)
    for temp in templates:
        if  temp["data"] is None:
            continue

        wb = app.books.open(temp["temp_path"])
        sht = wb.sheets[temp["sheet"]]
        
        sht.range(temp["cell"]).value = temp["data"]
        wb.save(temp["output"])
        wb.close()

    app.quit()
        
        

templates_with_data = {
    "工商银行": [
        {
            "temp_path": r"D:\企业补贴\银行报盘\工商银行报盘模板.xlsx",
            "sheet": "工行跨行",
            "cell": "A2",
            "data": conv_icbc(preprocess(
                df = read_dbf(file_path=gen_path(base_dir=r"D:\企业补贴\数据\老人企业补贴",term="202509",file_name="bt_ltx.dbf")),
                benifits_type= "老人企业补贴")),
            "output": gen_path(base_dir=r"D:\企业补贴\银行报盘",term="202509",file_name=f"{"老人企业补贴(工行报盘).xlsx"}")
        },
        {
            "temp_path": r"D:\企业补贴\银行报盘\工商银行报盘模板.xlsx",
            "sheet": "工行跨行",
            "cell": "A2",
            "data": conv_icbc(preprocess(
                df=read_dbf(file_path=gen_path(base_dir=r"D:\企业补贴\数据\集体工企业补贴",term="202509",file_name="bt_ltx.dbf")),
                benifits_type ="集体工企业补贴")
            ),
            "output": gen_path(base_dir=r"D:\企业补贴\银行报盘",term="202509",file_name=f"{"集体工企业补贴(工行报盘).xlsx"}")
        },
        {
            "temp_path": r"D:\企业补贴\银行报盘\工商银行报盘模板.xlsx",
            "sheet": "工行跨行",
            "cell": "A2",
            "data": conv_icbc(preprocess(
                df=read_dbf(file_path=gen_path(base_dir=r"D:\企业补贴\数据\中人提高待遇",term="202509",file_name="bt_ltx.dbf")),
                benifits_type="中人提高待遇")
            ),
            "output": gen_path(base_dir=r"D:\企业补贴\银行报盘",term="202509",file_name=f"{"中人提高待遇(工行报盘).xlsx"}")
        },
    ],
    "建设银行": [
        {
            "temp_path": r"D:\企业补贴\银行报盘\建设银行报盘模板.xlsx",
            "sheet": "sheet1",
            "cell": "A2",
            "data": conv_cbc(preprocess(
                df=read_dbf(file_path=gen_path(base_dir=r"D:\企业补贴\数据\集体工企业补贴",term="202509",file_name="bt_ltx.dbf")),
                benifits_type ="集体工企业补贴")
            ),
            "output": gen_path(base_dir=r"D:\企业补贴\银行报盘",term="202509",file_name=f"{"集体工企业补贴(建行报盘).xlsx"}")
        },
        {
            "temp_path": r"D:\企业补贴\银行报盘\建设银行报盘模板.xlsx",
            "sheet": "sheet1",
            "cell": "A2",
            "data": conv_cbc(preprocess(
                df=read_dbf(file_path=gen_path(base_dir=r"D:\企业补贴\数据\老人企业补贴",term="202509",file_name="bt_ltx.dbf")),
                benifits_type ="老人企业补贴")
            ),
            "output": gen_path(base_dir=r"D:\企业补贴\银行报盘",term="202509",file_name=f"{"老人企业补贴(建行报盘).xlsx"}")
        },
    ],
    "中国银行(油区)": [
        {
            "temp_path": r"D:\企业补贴\银行报盘\中国银行报盘模板.xlsx",
            "sheet": "sheet1",
            "cell": "A2",
            "data": conv_bocyt(preprocess(
                df=read_dbf(file_path=gen_path(base_dir=r"D:\企业补贴\数据\老人企业补贴",term="202509",file_name="bt_ltx.dbf")),
                benifits_type ="老人企业补贴")
            ),
            "output": gen_path(base_dir=r"D:\企业补贴\银行报盘",term="202509",file_name=f"{"老人企业补贴(中行油区报盘).xlsx"}")
        },
        {
            "temp_path": r"D:\企业补贴\银行报盘\中国银行报盘模板.xlsx",
            "sheet": "sheet1",
            "cell": "A2",
            "data": conv_bocyt(preprocess(
                df=read_dbf(file_path=gen_path(base_dir=r"D:\企业补贴\数据\集体工企业补贴",term="202509",file_name="bt_ltx.dbf")),
                benifits_type ="集体工企业补贴")
            ),
            "output": gen_path(base_dir=r"D:\企业补贴\银行报盘",term="202509",file_name=f"{"集体工企业补贴(中行油区报盘).xlsx"}")
        }
    ],
    "中国银行(南阳)": [
        {
            "temp_path": r"D:\企业补贴\银行报盘\中国银行南阳报盘模板.xlsx",
            "sheet": "sheet1",
            "cell": "A2",
            "data": conv_bocny(preprocess(
                df=read_dbf(file_path=gen_path(base_dir=r"D:\企业补贴\数据\老人企业补贴",term="202509",file_name="bt_ltx.dbf")),
                benifits_type ="老人企业补贴")
            ),
            "output": gen_path(base_dir=r"D:\企业补贴\银行报盘",term="202509",file_name=f"{"老人企业补贴(中行南阳报盘).xlsx"}")
        },
        {
            "temp_path": r"D:\企业补贴\银行报盘\中国银行南阳报盘模板.xlsx",
            "sheet": "sheet1",
            "cell": "A2",
            "data": conv_bocny(preprocess(
                df=read_dbf(file_path=gen_path(base_dir=r"D:\企业补贴\数据\集体工企业补贴",term="202509",file_name="bt_ltx.dbf")),
                benifits_type ="集体工企业补贴")
            ),
            "output": gen_path(base_dir=r"D:\企业补贴\银行报盘",term="202509",file_name=f"{"集体工企业补贴(中行南阳报盘).xlsx"}")
        }

    ],
}


if __name__ == "__main__":
    export_data(templates_with_data["工商银行"])
    export_data(templates_with_data["建设银行"])
    export_data(templates_with_data["中国银行(南阳)"])
    export_data(templates_with_data["中国银行(油区)"])
