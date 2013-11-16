scale-contest-evaluator
=======================

You can import the class by putting the file in the right folder and typing

    import scale
    
To run the algorithm on a file and calculate the score use:

    scale.main('week1.log')
    
To simulate events with a constant arrival rate for e.g. 300 seconds:

    simulation = scale.Simulation(300, {'url': 1.2, 'default': 0.7, 'export': 1.7})
    simulation.load_service_distribution('week1.log')
    for event in simulation.generator():
        print event
