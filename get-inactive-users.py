from datetime import datetime, timedelta
from time import gmtime, strftime
import requests as rq
import os
import json


def fetch_all_org_users(url, headers):
    get_users_url=url+"organization/member?limit=9999"
    response = rq.get(get_users_url, headers=headers)
    json_response = response.json()
    if response.status_code == 200:
        #Note that the return limit set to 9999"
        print("Total # of org users: ", json_response["count"])

        results=json_response["results"]
        org_members = []
        for i in range(len(results)):
            org_members.append(results[i]["email"])
        return org_members
    else:
        print(f"Failed to fetch data from API. Status Code: {json_response.status_code}")
        
def fetch_active_members(url,headers):
    sixty_days_ts=int((datetime.now() - timedelta(days=60)).timestamp() * 1000)
    active_session_url=url+"event/find?query=sf_eventType%3ASessionLog%20AND%20sessionType%3Auser&start_time="+str(sixty_days_ts)
    response = rq.get(active_session_url, headers=headers)
    json_response = response.json()
    if response.status_code == 200:
        active_members = []
        for i in range(len(json_response)):
            new_entry=json_response[i]["properties"]["email"]
            if new_entry not in active_members:
                active_members.append(new_entry)
        return active_members
    else:
        print(f"Failed to fetch data from API. Status Code: {json_response.status_code}")

def main():
    # get realm + access token
    realm=input("Enter the realm: ")
    token=input("Enter your user API access token for the org: ")
    url = "https://api."+realm+".signalfx.com/v2/"
    headers = {
        "Content-Type": "application/json",
        "X-SF-TOKEN": token
    }
    org_members=fetch_all_org_users(url,headers)
    active_members=fetch_active_members(url,headers)
    
    for user in active_members:
        org_members.remove(user)
    
    print("Total # of inactive users: ", len(org_members))
    print(org_members)


if __name__ == "__main__":
    main()
