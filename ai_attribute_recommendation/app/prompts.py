import json


system_message_attribute = """
    You are an AI trained to recommend attributes for customer segments.
    Always output valid JSON format and do not produce the exact same without analysis.
    """


prompt_attribute = """

Given the following categories and their attributes, identify the most relevant elements for the customer segment.

## Selection Criteria:
1. **Prioritize relevance** based on the given segment name and its likely needs.
2. **High-End Customers** (e.g., includes "VIP", "高端") → Select attributes with high spending and premium products.
3. **Loyal Customers** (e.g., includes "忠實", "常客") → Prioritize high purchase frequency and mid-range spending.
4. **Financial Segments (e.g., includes "金融", "證券", "ETF", "交易")** → Prioritize financial tools, high trading frequency, and related professional roles.


Segment name: '高端客戶'
### **Example Input (High-End Customer)**:
Categories, Attributes, and Sub-Attributes:
{{
    'EDM 屬性': 
        {{
        "已凍結": [ "是", "否" ]
    }} 
}}
### **Example Output (High-End Customer)**:
```json
Example:
{{
    "segment": "高端客戶",
    "selected_attributes":
        {{
        "EDM 屬性":
            {{
            "已凍結": [ 
                        "否"
                ]
        }}
    }}
}}
```

Segment name: '高頻交易VIP'
### **Example Input (High-Frequency Trading VIP)**:
Categories, Attributes, and Sub-Attributes:
{{
    '消費屬性': 
        {{
        "每月平均消費": [
                "8000",
                "11500",
                "6000",
                "10000",
                "12000",
                "5500",
                "9000",
                "8500",
                "7200",
                "6800"
            ],
    }} 
}}
### **Example Output (High-Frequency Trading VIP)**:
```json
Example:
{{
    "segment": "高頻交易VIP",
    "selected_attributes":
        {{
        "消費屬性":
            {{
            每月平均消費: [
                "12000",
                "11500",
            ]
        }}
    }}


Segment name:
'{segment_name}'
Data(Categories, Attributes, and Sub-Attributes): 
'{categories_dict}'

For the segment name and data, let's think step by step, think about what segment name means, and then generate a relevant JSON output based on the provided data(Categories, Attributes, and Sub-Attributes).

⚠️ IMPORTANT:
Always provide a non-empty "segment" field that matches the input.
If you are not sure about wehther the attribute is relevant, then leave the "selected_attributes" field empty.
If segment name does not include brand or company references (e.g., ‘英丰寶’, ‘FinTech Corp’), do NOT select `公司名稱`.
Do not generate Categories, Attributes, and Sub-Attributes yourself that are not in the Data(Categories, Attributes, and Sub-Attributes).
Ensure output is formatted in traditional Chinese JSON.
"""

system_message_explanation = """
    You are an AI trained to explain the reasoning behind the selection of attributes for customer segments.
    Always output valid JSON format.
    """

prompt_explanation = """
Given the following categories and their attributes recommended by the AI, explain why the attributes are relevant to the customer segment.

Recommended Attributes and segment name:
{categories_dict} 


### **Example Input**:
```json
Example:
{{
    "segment": "忠實客戶",
    "selected_attributes": {{
        "一般屬性": {{
            "性別": ["男", "女"],
            "職稱": ["專員", "設計師"],
            "資料來源": ["訂單記錄", "perry"],
            "部門": ["行銷", "財務"]
            "每月平均消費": ["9000", "10000"],
            "購物管道": ["手機"],
            "購買頻率": ["3次", "4次"]
        }},
        "消費屬性": {{
            "每月平均消費": ["9000", "10000"],
            "購買頻率": ["3次", "4次"]
        }}
    }}
}}
```
### **Example Output**:
```json
{{
    "explanation": "客群名稱'忠實客戶'指的是消費頻率高且對中價位產品有需求的客戶，因此選擇了中等消費金額和中價位產品。" ,
    "better_segment_name": "忠誠客群"
}}


Please provide a better segment name if necessary, if the segment name is already accurate, leave the "better_segment_name" field empty.

"""