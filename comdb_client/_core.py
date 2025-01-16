"""
@Function: ComDB Client Library
@Author : ZhangPeiCheng
@Time : 2025/1/15 19:11
"""

import requests

__all__ = ["ComDBClient"]

class ComDBClient:
    def __init__(self, ip="localhost", port=9090):
        """
        Initialize the ComDBClient instance.

        :param ip: The IP address of the ComDB server.
        :param port: The port number of the ComDB server.
        """
        self.addr = f"http://{ip}:{port}"

    def test_connection(self):
        """
        Test the connection to the ComDB server.

        :return: True if the connection is successful, False otherwise.
        """
        try:
            response = requests.get(f"{self.addr}/health")
            return response.status_code == 200
        except requests.RequestException as e:
            print(f"Connection test failed: {e}")
            return False

    def get(self, key):
        """
        Retrieve the value for a given key from the ComDB server.

        :param key: The key to retrieve.
        :return: The value associated with the key, or None if not found.
        """
        try:
            response = requests.get(f"{self.addr}/bitcask/get", params={"key": key})
            if response.status_code == 200:
                return response.text
            else:
                print(f"Failed to get key '{key}': {response.status_code} {response.text}")
                return None
        except requests.RequestException as e:
            print(f"Error during GET request: {e}")
            return None

    def put(self, key, value):
        """
        Store a key-value pair in the ComDB server.

        :param key: The key to store.
        :param value: The value to store.
        :return: True if the operation is successful, False otherwise.
        """
        try:
            # 构造符合服务器端期望的请求体
            data = {key: value}
            # 发送POST请求
            response = requests.post(f"{self.addr}/bitcask/put", json=data)

            # 检查响应状态码
            if response.status_code == 200:
                return True
            else:
                print(f"Failed to put key '{key}': {response.status_code} {response.text}")
                return False
        except requests.RequestException as e:
            print(f"Error during PUT request: {e}")
            return False

    def listKey(self):
        """
        List all key stored in the comDB server
        :return: True if the operation is successful, False otherwise.
        """
        try:
            response = requests.get(f"{self.addr}/bitcask/listkeys")
            if response.status_code == 200:
                return response.text
            else:
                print( response.status_code)
                print("Failed to list keys")
                return False
        except requests.RequestException as e:
            print(f"Error during listkeys request: {e}")
            return False

    def Stat(self):
        """
        get the status of the ComDB server
        :return: True if the operation is successful, False otherwise.
        """
        try:
            response = requests.get(f"{self.addr}/bitcask/stat")
            if response.status_code == 200:
                return response.text
            else:
                print("Failed to state the database")
                return False
        except requests.RequestException as e:
            print(f"Error during stat request: {e}")
            return False

    def delete(self, key):
        """
        delete data
        :param key:  key。
        """
        try:
            response = requests.delete(f"{self.addr}/bitcask/delete", params={"key": key})
            if response.status_code == 200:
                print(f"Key '{key}' deleted successfully.")
                return True
            else:
                print(f"Failed to delete key '{key}': {response.status_code} {response.text}")
                return False
        except requests.RequestException as e:
            print(f"Error during DELETE request: {e}")
            return False
    def get_by_prefix(self, prefix):
        """
          Retrieve the value for a given key from the ComDB server.

          :param prefix: The prefix of the key
          :return: The value associated with the key, or None if not found.
      """
        try:
            response = requests.get(f"{self.addr}/bitcask/prefix", params={"prefix": prefix})
            if response.status_code == 200:
                return response.text
            else:
                print(f"Failed to get prefix '{prefix}': {response.status_code} {response.text}")
                return None
        except requests.RequestException as e:
            print(f"Error during PREFIX request: {e}")
            return None
    def memory_get(self,agentId):
        """
        Gain all the memory of the agentId
        notice: if you set the memorySize with a large number. it may cause some unpredictable err.
        :param agentId: the unique label of the agent in the database,you can manage the memory space by agentId
        :return: The value associated with the key, or None if not found.
        """
        try:
            response = requests.get(f"{self.addr}/memory/get", params={"agentId": agentId})
            if response.status_code == 200:
                return response.text
            else:
                print(f"Failed to get memory from agentId:'{agentId}': {response.status_code} {response.text}")
                return None
        except requests.RequestException as e:
            print(f"Error during MEMORY/GET request: {e}")
            return None

    def memory_set(self, agent_id, value):
        """
        set memory

        :param agent_id: agent unique label。
        :param value: value。
        """
        try:
            data = {
                "agentId": agent_id,
                "value": value
            }

            response = requests.post(f"{self.addr}/memory/set", json=data)

            if response.status_code == 200:
                print(f"Value stored successfully for agent '{agent_id}'.")
                return True
            else:
                print(f"Failed to store value for agent '{agent_id}': {response.status_code} {response.text}")
                return False
        except requests.RequestException as e:
            print(f"Error during memory set request: {e}")
            return False

    def memory_search(self, agent_id, search_item):
        """
        Search the memory space of the specified agent for the given search item.

        :param agent_id: The unique identifier of the agent.
        :param search_item: The search query to match against the agent's memory.
        :return: The search results if successful, otherwise None.
        """
        try:
            # Send a GET request to the /memory/search endpoint
            response = requests.get(
                f"{self.addr}/memory/search",
                params={"agentId": agent_id, "searchItem": search_item}
            )

            # Check the response status code
            if response.status_code == 200:
                # Parse and return the search results
                return response.json()
            else:
                print(f"Failed to search memory for agent '{agent_id}': {response.status_code} {response.text}")
                return None
        except requests.RequestException as e:
            print(f"Error during memory search request: {e}")
            return None

    def create_memory_meta(self, agent_id, total_size):
        """
        Create a new memory space for the specified agent.

        :param agent_id: The unique identifier of the agent.
        :param total_size: The total size of the memory space to create.
        :return: The created memory metadata if successful, otherwise None.
        """
        try:
            # Construct the request body
            data = {
                "agentId": agent_id,
                "totalSize": total_size
            }

            # Send a POST request to the /memory/create endpoint
            response = requests.post(f"{self.addr}/memory/create", json=data)

            # Check the response status code
            if response.status_code == 200:
                # Parse and return the created memory metadata
                return response.json()
            else:
                print(f"Failed to create memory space for agent '{agent_id}': {response.status_code} {response.text}")
                return None
        except requests.RequestException as e:
            print(f"Error during memory creation request: {e}")
            return None

    def compress_memory(self, agent_id, endpoint):
        """
        Compress the memory space of the specified agent using the provided compression endpoint.

        :param agent_id: The unique identifier of the agent.
        :param endpoint: The compression endpoint to use for compressing the memory.
        :return: True if the compression was successful, otherwise False.
        """
        try:
            # Construct the request body
            data = {
                "agentId": agent_id,
                "endpoint": endpoint
            }

            # Send a POST request to the /memory/compress endpoint
            response = requests.post(f"{self.addr}/memory/compress", json=data)

            # Check the response status code
            if response.status_code == 200:
                # Parse the response to check if compression was successful
                result = response.json()
                if result.get("success", False):
                    print(f"Memory compression successful for agent '{agent_id}'.")
                    return True
                else:
                    print(f"Memory compression failed for agent '{agent_id}'.")
                    return False
            else:
                print(f"Failed to compress memory for agent '{agent_id}': {response.status_code} {response.text}")
                return False
        except requests.RequestException as e:
            print(f"Error during memory compression request: {e}")
            return False

    def create_compressor(self, agent_id, endpoint):
        """
        Create a compressor for the specified agent's memory space.

        :param agent_id: The unique identifier of the agent.
        :param endpoint: The compression endpoint to use for the compressor.
        :return: True if the compressor was created successfully, otherwise False.
        """
        try:
            # Construct the request body
            data = {
                "agentId": agent_id,
                "endpoint": endpoint
            }

            # Send a POST request to the /memory/create-compressor endpoint
            response = requests.post(f"{self.addr}/memory/create-compressor", json=data)

            # Check the response status code
            if response.status_code == 200:
                print(f"Compressor created successfully for agent '{agent_id}'.")
                return True
            else:
                print(f"Failed to create compressor for agent '{agent_id}': {response.status_code} {response.text}")
                return False
        except requests.RequestException as e:
            print(f"Error during compressor creation request: {e}")
            return False


client = ComDBClient()


# # Example usage
if __name__ == "__main__":
    client = ComDBClient("172.31.88.128", 9090)

    # Test the connection
    if client.test_connection():
        print("Connection successful!")
    else:
        print("Failed to connect to ComDB server.")

    # Perform some operations
    if client.put("czp", "ZhangPeiCheng"):
        print("Successfully stored the key-value pair.")

    value = client.get("czp")
    if value:
        print(f"Retrieved value: {value}")
    else:
        print("Failed to retrieve the value.")

    keys = client.listKey()
    print(keys)

    client.put("czp1", "ZhangPeiCheng1")
    client.put("czp2", "ZhangPeiCheng2")
    client.put("czp3", "ZhangPeiCheng3")
    client.put("czp4", "ZhangPeiCheng4")
    result = client.get_by_prefix("czp")
    print(result)

    agentId = "114514"
    # # insert into the memory
    text_message = [
        "In the field of artificial intelligence, training deep learning models requires a large amount of data and computational resources. To improve model performance, researchers often use distributed training techniques, distributing tasks across multiple GPUs or TPUs for parallel processing. This approach can significantly reduce training time, but it also introduces challenges related to data synchronization and communication overhead.",
        "Natural Language Processing (NLP) is an important branch of artificial intelligence, focusing on enabling computers to understand and generate human language. In recent years, Transformer-based models (such as BERT and GPT) have made significant progress in NLP tasks. These models capture contextual information in text through self-attention mechanisms, leading to outstanding performance in various tasks.",
        "Cloud computing is a core component of modern IT infrastructure, allowing users to access computing resources, storage, and applications over the internet. Cloud service providers (such as AWS, Azure, and Google Cloud) offer elastic scaling and pay-as-you-go models, enabling businesses to manage and deploy their IT resources more efficiently.",
        "Blockchain technology is a decentralized distributed ledger technology widely used in cryptocurrencies (such as Bitcoin and Ethereum). Blockchain ensures data security and immutability through cryptographic algorithms, while achieving decentralized trust mechanisms through consensus mechanisms (such as PoW and PoS).",
        "The Internet of Things (IoT) refers to connecting various physical devices through the internet, enabling them to communicate and collaborate with each other. IoT technology has broad applications in smart homes, industrial automation, and smart cities. Through sensors and data analysis, IoT helps businesses and individuals achieve more efficient resource management and decision-making support.",
        "Quantum computing is a computational model based on the principles of quantum mechanics, with the potential to surpass classical computers. Quantum bits (qubits) can exist in multiple states simultaneously, allowing quantum computers to process large amounts of data in parallel. Although quantum computing is still in its early stages, it shows great promise in fields such as cryptography, materials science, and drug development.",
        "Edge computing is a computing paradigm that shifts computational tasks from centralized cloud systems to edge devices closer to the data source. Edge computing can reduce data transmission latency, improve real-time performance, and lower bandwidth consumption. It has significant applications in autonomous driving, industrial IoT, and smart cities.",
        "Data science is an interdisciplinary field that combines statistics, computer science, and domain knowledge to extract valuable insights from data. Data scientists use machine learning, data mining, and visualization tools to analyze data, helping businesses make data-driven decisions. Data science has wide applications in finance, healthcare, and marketing.",
        "DevOps is a software development methodology that emphasizes collaboration and automation between development and operations teams. Through continuous integration, continuous delivery, and automated testing, DevOps can significantly improve the efficiency and quality of software development. The DevOps culture also encourages rapid iteration and continuous improvement to meet rapidly changing market demands.",
        "Reinforcement learning is a machine learning approach that trains agents through trial and error and reward mechanisms. Reinforcement learning has broad applications in game AI, robotics control, and autonomous driving. Unlike supervised and unsupervised learning, reinforcement learning does not require pre-labeled data but learns optimal strategies through interaction with the environment."
    ]

    # All matching messages are placed in a list in parallel
    match_message = [
        "Distributed training techniques for deep learning models can significantly reduce training time but also introduce challenges related to data synchronization and communication overhead. Optimizing the efficiency of distributed training is an important research direction.",
        "Transformer models in natural language processing capture contextual information in text through self-attention mechanisms, leading to outstanding performance in various tasks. BERT and GPT are representative models in this field.",
        "Cloud computing enables businesses to manage and deploy their IT resources more efficiently through elastic scaling and pay-as-you-go models. AWS, Azure, and Google Cloud are major cloud service providers.",
        "Blockchain technology ensures data security and immutability through cryptographic algorithms, while achieving decentralized trust mechanisms through consensus mechanisms. Bitcoin and Ethereum are typical applications of blockchain technology.",
        "IoT technology helps businesses and individuals achieve more efficient resource management and decision-making support through sensors and data analysis. Smart homes and smart cities are important application scenarios for IoT.",
        "Quantum computing, based on the principles of quantum mechanics, has the potential to surpass classical computers. Quantum bits can exist in multiple states simultaneously, enabling quantum computers to process large amounts of data in parallel."
    ]

    if client.create_memory_meta(agentId,10):
        print("create successfully")
    for message in text_message:
        if client.memory_set(agentId, message) is False:
            print("memory message insert error")
    memory = client.memory_get(agentId)
    print(memory)
    for test in match_message:
        result = client.memory_search(agentId, test)
        print(result)

    client.compress_memory(agentId,"http://172.24.216.71:5000/generate")
    result = client.Stat()
    print(result)


