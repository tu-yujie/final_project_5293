# please install pymango and uninstall bson
import bson # this package is from pymango
import os
from utility import download_file, extract_data
import pandas as pd

url = "http://ghtorrent-downloads.ewi.tudelft.nl/mongo-daily/mongo-dump-2019-06-30.tar.gz"

# download the zipped file from the original source
file_name = download_file(url)
raw_data_path = "./raw_data/"
processed_data_path = "./data/"

if not os.path.exists(raw_data_path):
    extract_data(raw_data_path, file_name)

if not os.path.exists(processed_data_path):
    os.mkdir(processed_data_path)

print("processing pull_request_comments.bson")
data_generator = bson.decode_file_iter(open(raw_data_path + "dump/github/pull_request_comments.bson", "rb"))
data = {d["id"]: {"user_id": d["user"]["login"], "author_association": d["author_association"], 
        "created_at": d["created_at"], "body": d["body"], "url": d["html_url"], "repo": d["repo"]}
        for d in data_generator}
pd.DataFrame.from_dict(data,orient='index').to_csv(processed_data_path + "pull_request_comments.csv")
print("successfully processed commits.bson and stored it in {}".format(processed_data_path + "pull_request_comments.csv"))

print("processing issue_comments.bson")
data_generator = bson.decode_file_iter(open(raw_data_path + "dump/github/issue_comments.bson", "rb"))
data = {d["id"]: {"user_id": d["user"]["login"], "author_association": d["author_association"],
                  "created_at": d["created_at"], "body": d["body"], "url": d["html_url"], "repo": d["repo"]} 
        for d in data_generator}
pd.DataFrame.from_dict(data,orient='index') 
print("successfully processed commits.bson and stored it in {}".format(processed_data_path + "issue_comments.csv"))

print("processing commits.bson")
data_generator = bson.decode_file_iter(open(raw_data_path + "dump/github/commits.bson", "rb"))
data = {}
for d in data_generator:
    user = d["author"]["login"] if d["author"] else ""
    stats = d["stats"] if "stats" in d else ""
    data[d["sha"]] = {"user": user, "committer": d["commit"]["committer"]["name"],
                      "time": d["commit"]["author"]["date"], 'message': d["commit"]["message"], 
                      "stats": stats, "url": d["html_url"]} 
pd.DataFrame.from_dict(data,orient='index').to_csv(processed_data_path + "commits.csv")
print("successfully processed commits.bson and stored it in {}".format(processed_data_path + "commits.csv"))