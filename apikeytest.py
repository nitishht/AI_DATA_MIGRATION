# import requests
# import os
# import sys
# import json

# # More secure way
# # incubator_endpoint = os.getenv("EYQ_INCUBATOR_ENDPOINT")
# # incubator_key = os.getenv("EYQ_INCUBATOR_KEY")

# # Less secure way
# incubator_endpoint = "https://eyq-incubator.america.fabric.ey.com/eyq/us/api"
# incubator_key = "tbYVUCGUBA9YbHVoYHTOIKhgufmPf2LX"

# model = "gpt-5"  # Replace with desired model

# api_version = "2025-04-16"

# headers = {
#     "api-key": incubator_key
# }

# query_params = {
#     "api-version": api_version
# }

# print("\n\nEYQ Incubator\n\n")
# try:
#     while True:
#         prompt = input("[You]: ")

#         body = {
#             "messages":[
#                 {"role": "system", "content": "You are a helpful assistant that only replies to questions about EY."},
#                 {"role":"user","content": prompt}
#             ]
#         }

#         full_path = incubator_endpoint + "/openai/deployments/" + model + "/chat/completions"

#         response = requests.post(full_path, json=body, headers=headers, params=query_params)

#         status_code = response.status_code

#         response1 = response.json()

#         if status_code == 200:
#             body["messages"].append({"role": "system", "content": response1["choices"][0]["message"]["content"]})
#         else:
#             print("\nError: ", status_code)
#             print("Response:", json.dumps(response1, indent=2))
#             break

#         print("\n" + "[EYQ-Incubator]: " + response1["choices"][0]["message"]["content"] + "\n")
# except KeyboardInterrupt:
#     print("\n\n[!] Exiting...\n")
#     sys.exit(0)

import os, requests
from dotenv import load_dotenv
load_dotenv()

params = {"api-version": "2024-10-21"}  # or keep "2025-04-16" if your APIM requires it
api_key = os.getenv("AZURE_OPENAI_API_KEY")

if not api_key:
    raise RuntimeError("AZURE_OPENAI_API_KEY is not set in the environment.")

headers = {
    "api-key": api_key,                 # <- critical: APIM requires this header name
    "Accept": "application/json",
    "Content-Type": "application/json"
}

body = {
    "messages": [{"role": "user", "content": "ping"}]
}

r = requests.post(url, params=params, headers=headers, json=body)
print(r.status_code, r.text)