# 导入必要的库
import json  # 用于处理JSON数据
import tempfile  # 用于创建临时文件
import csv  # 用于处理CSV文件
import streamlit as st  # 用于创建Web应用
import pandas as pd  # 用于数据处理和分析
from phi.model.openai import OpenAIChat  # OpenAI聊天模型
from phi.agent.duckdb import DuckDbAgent  # DuckDB数据库代理
from phi.tools.duckdb import DuckDbTools  # DuckDB工具
import re  # 正则表达式库

# 预处理并保存上传文件的函数
def preprocess_and_save(file):
    try:
        # 根据文件扩展名读取文件为DataFrame
        if file.name.endswith('.csv'):
            # 处理CSV文件，设置编码和缺失值标识
            df = pd.read_csv(file, encoding='utf-8', na_values=['NA', 'N/A', 'missing'])
        elif file.name.endswith('.xlsx'):
            # 处理Excel文件，设置缺失值标识
            df = pd.read_excel(file, na_values=['NA', 'N/A', 'missing'])
        else:
            # 不支持的文件格式提示
            st.error("Unsupported file format. Please upload a CSV or Excel file.")
            return None, None, None
        
        # 确保字符串列被正确引用
        for col in df.select_dtypes(include=['object']):
            # 替换双引号为两个双引号，避免CSV解析问题
            df[col] = df[col].astype(str).replace({r'"': '""'}, regex=True)
        
        # 解析日期和数字列
        for col in df.columns:
            if 'date' in col.lower():
                # 尝试将包含'date'的列转换为日期时间类型
                df[col] = pd.to_datetime(df[col], errors='coerce')
            elif df[col].dtype == 'object':
                try:
                    # 尝试将对象类型列转换为数字类型
                    df[col] = pd.to_numeric(df[col])
                except (ValueError, TypeError):
                    # 转换失败时保持原类型
                    pass
        
        # 创建临时文件保存预处理后的数据
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
            temp_path = temp_file.name
            # 将DataFrame保存为CSV文件，所有字段加引号
            df.to_csv(temp_path, index=False, quoting=csv.QUOTE_ALL)
        
        return temp_path, df.columns.tolist(), df  # 返回临时路径、列名列表和DataFrame
    except Exception as e:
        # 异常处理，显示错误信息
        st.error(f"Error processing file: {e}")
        return None, None, None

# 主应用部分
st.title("📊 Data Analyst Agent")  # 设置应用标题

# 侧边栏用于API密钥输入
with st.sidebar:
    st.header("API Keys")
    # 输入OpenAI API密钥，密码类型隐藏输入内容
    openai_key = st.text_input("Enter your OpenAI API key:", type="password")
    if openai_key:
        # 保存API密钥到会话状态
        st.session_state.openai_key = openai_key
        st.success("API key saved!")
    else:
        st.warning("Please enter your OpenAI API key to proceed.")

# 文件上传组件
uploaded_file = st.file_uploader("Upload a CSV or Excel file", type=["csv", "xlsx"])

# 当上传文件且API密钥已设置时执行
if uploaded_file is not None and "openai_key" in st.session_state:
    # 预处理并保存上传的文件
    temp_path, columns, df = preprocess_and_save(uploaded_file)
    
    if temp_path and columns and df is not None:
        # 显示上传的数据表格
        st.write("Uploaded Data:")
        st.dataframe(df)  # 使用交互式表格显示数据
        
        # 显示上传数据的列名
        st.write("Uploaded columns:", columns)
        
        # 配置语义模型，指定数据表信息
        semantic_model = {
            "tables": [
                {
                    "name": "uploaded_data",
                    "description": "Contains the uploaded dataset.",
                    "path": temp_path,
                }
            ]
        }
        
        # 初始化DuckDbAgent用于SQL查询生成
        # 修复：使用 phidata 库的正确组件
        duckdb_agent = DuckDbAgent(
            model=OpenAIChat(id="gpt-4o", api_key=st.session_state.openai_key),
            semantic_model=json.dumps(semantic_model),
            tools=[DuckDbTools()],
            markdown=True,
            add_history_to_messages=False,  # 禁用聊天历史
            followups=False,  # 禁用后续查询
            read_tool_call_history=False,  # 禁用读取工具调用历史
            system_prompt="You are an expert data analyst. Generate SQL queries to solve the user's query. Return only the SQL query, enclosed in ```sql ``` and give the final answer.",
        )
        
        # 在会话状态中初始化代码存储
        if "generated_code" not in st.session_state:
            st.session_state.generated_code = None
        
        # 主查询输入框
        user_query = st.text_area("Ask a query about the data:")
        
        # 添加终端输出提示信息
        st.info("💡 Check your terminal for a clearer output of the agent's response")
        
        # 提交查询按钮
        if st.button("Submit Query"):
            if user_query.strip() == "":
                st.warning("Please enter a query.")
            else:
                try:
                    # 处理查询时显示加载动画
                    with st.spinner('Processing your query...'):
                        # 获取DuckDbAgent的响应
                        response1 = duckdb_agent.run(user_query)

                        # 从响应对象中提取内容
                        if hasattr(response1, 'content'):
                            response_content = response1.content
                        else:
                            response_content = str(response1)
                        # 打印响应内容
                        response = duckdb_agent.print_response(
                            user_query,
                            stream=True,
                        )

                    # 在Streamlit中显示响应
                    st.markdown(response_content)
                
                except Exception as e:
                    # 异常处理，显示错误信息
                    st.error(f"Error generating response from the DuckDbAgent: {e}")
                    st.error("Please try rephrasing your query or check if the data format is correct.")

                    