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
        response = requests.post("https://login.microsoftonline.com/{}/oauth2/token".format(self.tenant),
                                 headers=self.headers,
                                 data=self.body)
        self.access_token = response.json()
        self.access_token = self.access_token['access_token']
        return self.access_token

    def getGroups(self):
        groups = requests.get(url=f'https://api.powerbi.com/v1.0/myorg/groups',
                              headers={'Authorization': f'Bearer {self.access_token}'})
        return groups.json()

    def getDatasets(self):
        list_of_groups = self.getGroups()
        group_list = [item['id'] for item in list_of_groups['value']]
        dataset_list = []
        for group in group_list:
            datasets = requests.get(url=f'https://api.powerbi.com/v1.0/myorg/groups/{group}/datasets',
                                    headers={'Authorization': f'Bearer {self.access_token}'})
            dataset_json = datasets.json()['value']
            for i in range(len(dataset_json)):
                dataset_json[i].update({"keys": group})
                dataset_list.append(dataset_json[i])
        return dataset_list

    def getRefreshHistory(self):
        datasets = self.getDatasets()
        dataset_list = [item['id'] for item in datasets]
        list_of_refreshes = []
        for dataset in dataset_list:
            try:
                dataset_id = dataset
                dataset = requests.get(url=f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes',
                                       headers={'Authorization': f'Bearer {self.access_token}'})
                dt = dataset.json()['value']
                for item in dt:
                    item.update({"keys": dataset_id})
                    list_of_refreshes.append(item)
            except:
                # Output is known and thats why PASS is allowed
                pass
        return list_of_refreshes




