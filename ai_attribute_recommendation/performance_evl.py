import cProfile
import pstats
import io
from logger import init_log

logger = init_log('performance_test')

def profile_function(fun, func_name, *args, **kwargs):
    def run_computation():
        result = fun(*args, **kwargs)  # Call the function with arguments
        return result

    # Profile the function
    with cProfile.Profile() as profiler:
        result = run_computation()

    # Create a stats object to analyze the profiler results
    stats = pstats.Stats(profiler)
    
    # Extract the total number of calls and the total time
    total_calls = stats.total_calls
    total_time = stats.total_tt
    
    # stats.sort_stats(pstats.SortKey.TIME)  # Sort by time spent
    # stats.print_stats(10)  # Print the top 10 functions by time spent
    
    # Log the summary information
    logger.info(f'=========Profiling Summary=========')
    logger.info(f'Total function calls: {total_calls}')
    logger.info(f'[{func_name}] Total time taken: {total_time:.3f} seconds')
    logger.info(f'=========Profiling Summary(END)=========')

    return result
