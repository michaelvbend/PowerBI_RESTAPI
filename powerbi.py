import json, requests

class PowerBIAPI:
    def __init__(self, tenant, client, username, password, client_secret):
        self.tenant = tenant
        self.body = {
            "grant_type": "password",
            "resource": "https://analysis.windows.net/powerbi/api",
            "client_id": client,
            "username": username,
            "password": password,
            "client_secret": client_secret
        }

        self.headers = {"Content-Type": "application/x-www-form-urlencoded"}
        self.connect()

    def connect(self):
        response = requests.post("https://login.microsoftonline.com/{}/oauth2/token".format(self.tenant), headers=self.headers,
                                 data=self.body)
        self.access_token = response.json()
        self.access_token = self.access_token['access_token']
        return self.access_token

    def getGroups(self):
        groups = requests.get(url=f'https://api.powerbi.com/v1.0/myorg/groups', headers={'Authorization': f'Bearer {self.access_token}'})
        return groups.json()

    def getDatasets(self, **kwargs):
        groupId = '75d2eec1-3b59-4edd-b982-19d90062e4bc'
        groups = requests.get(url=f'https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets', headers={'Authorization': f'Bearer {self.access_token}'})
        return groups.json()

    def getRefreshHistory(self):
        datasets = self.getDatasets()['value']
        list_of_datasets = [item['id'] for item in datasets]
        list_of_refreshes = []
        for dataset in list_of_datasets:
            try:
                dataset_id = dataset
                dataset = requests.get(url=f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes', headers={'Authorization': f'Bearer {self.access_token}'})
                dt = dataset.json()['value']
                for item in dt:
                    item.update({"keys": dataset_id})
                    list_of_refreshes.append(item)
            except:
                # Output is known and thats why PASS is allowed
                pass
        return list_of_refreshes





