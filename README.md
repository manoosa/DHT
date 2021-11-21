# DHT
Implement using Python 3 the Chord system as described in the original paper (Links to an external site.) from 2001 by Stoica, Morris, Karger,  Kaashoek, and Balakrishna. Do not do the enhancements in Section 5 of the paper, but instead rigorously update all the finger tables affected by a newly joined or removed node as described in Section 4. https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf

As a data set, we will store each row from some National Football League passing statistics file  Download passing statistics file(source: nfl.com via kaggle.com). Treat the value in the first column (playerid) concatenated with the fourth column (year) as the key and use SHA-1 to hash it. If there are any duplicates, write them both to the DHT (so the last row wins). For the nodes, use the string of its endpoint name, including port number, as its name and use SHA-1 to hash it (similar to what is suggested in the Stoica, et al. paper). The other columns for the row can be put together in a dictionary and returned when that key is fetched.

Your system should allow any node to join the group when there is no data yet in the DHT, but the other nodes do need to update their finger tables as shown in the paper (without the optimizations). There is an extra credit opportunity to have the new node take over the necessary keys and associated data when there is already data in the DHT. You are not required to handle a leaving node. A new node should be able to inform any arbitrary node in the current network.

Your system should allow a querier to talk to any arbitrary node in the network to query a value for a given key or add a key/value pair (with replacement). No need to handle removing a key for this lab. Nothing elaborate is required for "knowing" a node currently in the network. Any simple scheme even including just reading the printouts and spying a port number and then using that port number to talk to one of the nodes in the network. What is required is that I can arbitrarily choose any node and do either my query or my add using that node.

So to hand this in, I'd expect three files: chord_node.py, chord_populate.py, and chord_query.py. Command line arguments for the three programs are as follows:

chord_node takes a port number of an existing node (or 0 to indicate it should start a new network). This program joins a new node into the network using a system-assigned port number for itself. The node joins and then listens for incoming connections (other nodes or queriers). You can use blocking TCP for this and pickle for the marshaling.
chord_populate takes a port number of an existing node and the filename of the data file
chord_query takes a port number of an existing node and a key (any value from column 1+4 of the file)
