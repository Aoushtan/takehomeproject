# takehomeproject
Description
*	PyMongo application that connects to a MongoDB instance running through a docker container at localhost:27017
*	Uses .json files for example data and loads them in automatically on first run if the local mongo instance exists
*	Uses .ini to handle user input for filtering queries
*	Runs data through a pipeline of retrieving, aggregating, and storing as a new collection in the database
*	main.py is the main application, the .json files are used to load in data, and the .ini files are used for filtering and are the provided unit tests

Limitations
*	I had some trouble with making an intuitive user input system but I think the .ini file works good enough for this project
*	There is no MAP stage necessarily in this code due to the way I handled the retrieve step.
*	Application really isn't built for MongoDb data due to using .json files with present columns
	*	Also there isn't any logic built in for "or" in the queries as I didn't see a good way to do it with the .ini file and there are various other operations that aren't really supported like grouping by multiple fields
*	A lot more unit tests could be run to test every combination of .ini file configurations
*	Definitely needs more exception handling
*	Still definitely have more to learn about mongo pipelines as they are much different logically than SQL operations

Overall, I really enjoyed the project.  This was a lot of learning and applying what I've learned but was really the most fun I've had coding in a while.  I think in total I put somewhere between 20 and 24 hours into this.  I think for the time invested I'm pretty happy with it but can definitely see things I'd want to improve.