import json
from openai import OpenAI

class DatasetBundler:
    def __init__(self, client: OpenAI):
        self.client = client

    def choose_files(self, family: str, file_list: list, query: str):
        if len(file_list) > 25:
            file_list = file_list[:25]
        prompt = f"""
You are a dataset file selector.
Given files from dataset family "{family}", pick the ones relevant to the query.

Files:
{json.dumps(file_list, indent=2)}

Query: "{query}"

Return only JSON:
{{"selected_files": ["<file_id>", ...]}} or {{"selected_files": [-1]}}
"""
        res = self.client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        try:
            return json.loads(res.choices[0].message.content)
        except Exception:
            return {"selected_files": [-1]}
