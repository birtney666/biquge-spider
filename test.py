import re
import time

with open("./11.html","r",encoding="utf-8") as f:
   html_content = f.read()

matched = re.findall(r"<div id=\"chaptercontent\" class=\"Readarea ReadAjax_content\">(.*?)</div>", html_content, re.S)

if matched:
   content = matched[0]  # 获取第一个匹配结果
   
   # 移除script标签及其内容
   content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.S | re.I)
   
   # 移除特定的干扰元素
   content = re.sub(r'<p class="readinline">.*?</p>', '', content, flags=re.S)
   
   # 先按<br/>标签分割成段落
   paragraphs = re.split(r'<br\s*/?>', content, flags=re.I)
   
   # 清理每个段落
   cleaned_paragraphs = []
   for paragraph in paragraphs:
       # 移除HTML标签
       paragraph = re.sub(r'<[^>]+>', '', paragraph)

       # 清理空白字符
       paragraph = paragraph.strip()
       
       # 只保留非空段落
       if paragraph:
           cleaned_paragraphs.append(paragraph)
   
   # 移除作者注释和网站信息（在最后一段）
   if cleaned_paragraphs:
       last_paragraph = cleaned_paragraphs[-1]
       last_paragraph = re.sub(r'请收藏本站：.*$', '', last_paragraph, flags=re.S)
       last_paragraph = re.sub(r'『.*?』', '', last_paragraph)
       last_paragraph = last_paragraph.strip()
       
       if last_paragraph:
           cleaned_paragraphs[-1] = last_paragraph
       else:
           cleaned_paragraphs.pop()  # 如果最后一段清理后为空，则删除
   
   # 输出每一段
   for i, paragraph in enumerate(cleaned_paragraphs, 1):
       print(f"第{i}段: {paragraph}")
       
   print(f"\n总共{len(cleaned_paragraphs)}段")
   
else:
   print("未找到匹配内容")
