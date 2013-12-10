CATEGORIES = ['url', 'default', 'export']
CATEGORY = 'default'
for chromosome in chromosomes:
    # Initialize the algorithm
    scale = Scale(category, 10) # what category, and how many machines are we starting with?
    # Initialize the simulation
    evaluator = Evaluator()
    jobs = parse(f)
    for job in jobs:
        if job.category == CATEGORY: # Give the category you want to get a score for
            time, data = scale.get_state(job) # 'data' will have the current time, and the number of machines runnning
            instructions = dict((category, 0) for category in CATEGORIES)
            instructions[CATEGORY] = DecodeChromosome(time, data[CATEGORY]) # instructions on how many machines are required
            commands = scale.process(instructions) # feed the instructions to the algorithm, which will decide on whether to start or stop machines
            for command in commands: # now feed all the events to the simulation
                evaluator.receive(command) # run the simulation
            evaluator.receive(job)
            if True in evaluator.overwait.values():
                terminated_early = True
                running_time = evaluator.now - scale.starting_time + 120
                break
    bill, penalty, failed = Evaluator.score() # dictionaries
    if terminated_early:
        bill[CATEGORY], penalty[CATEGORY] = bill[CATEGORY] * 172800 / running_time, penalty[CATEGORY] * 172800 / running_time
        # Add a constant
    fitness = 1 / (bill[CATEGORY] + penalty[CATEGORY])
