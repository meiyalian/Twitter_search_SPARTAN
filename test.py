from mpi4py import MPI
import json
import sys



def convert_to_json(line_number, data ):
    """
        convert a line in the twitter data file into json format 
    """
    if line_number > 0: #ignore the first line of file 
        data = data.strip()
        if data[-2] == "]":
            data = data[:-2]
        else:
            if data[-1] == ",":
                data = data[:-1]
        return data



def process_tweets(data_file, rank, no_of_process, grids_arr, score_counter ):
    """
        This function read the twitter data file and process certain lines based on the rank 
        of the process. 

        input: data_file(string), rank(int>=0),  no_of_process(int >=1), grids_arr(list), score_counter( ScoreCounter object)
        output: a dict contains the total number of tweets and the corresponding scores for each cell area
     """
    statistics = dict()
    for grid in grids_arr:
        statistics[grid["id"]] = [0,0] # each cell in dict stores [number_of_tweets, overall_score ]

    with open(data_file, "r") as f:
        for i, line in enumerate(f):
            if i % no_of_process == rank:
                try:
                    tweet = convert_to_json(i, line)
                    if tweet is not None: 
                        tweet = json.loads(tweet)
                        location = locate_coord(grids_arr, tweet["value"]["geometry"]["coordinates"])
                        if location is not None: #if the location is in one of the given cell, calculate its score
                            score = score_counter.countScore(tweet["value"]["properties"]["text"])
                            previous = statistics.get(location)
                            statistics[location] = [previous[0]+1, previous[1]+score ]
            
                except ValueError:
                    pass

        return statistics



def parse_grid(fname):
    """
        This function parse the melbGrid file and sort the grids based on the left-down priority.

        input: fname(string)
        output: an array of sorted grids
    """
    grids = []
    row_boundaries = list()
    col_boundaries = list()
    with open(fname) as f:
        data = json.load(f)["features"]
        for each in data:
            grids.append(each["properties"])
        
    grids.sort(key=lambda g: g["ymin"], reverse= False)
    grids.sort(key=lambda g: g["xmin"], reverse= False)

    return grids

def locate_coord(grids, coord):
    """
        This function takes a coordinate and determine which cell does it belong to

        input: grids ( grids array), coord ( coordinates, [x_coord, y_coord])
        output: the cell it belongs to(None if it doesnt belong to any)
    """
    for i in range(len(grids)):
        if coord[0]>= grids[i]["xmin"] and coord[0] <=grids[i]["xmax"] and coord[1]>=grids[i]["ymin"] and coord[1] <= grids[i]["ymax"]:
            return grids[i]["id"]




class Node:
    def __init__(self):
        # self.val = value
        self.children = dict()
        self.index = 0 
        self.score = 0
        self.is_word = False
    

class Trie:
    def __init__(self):
        self.root = Node()
    
    def add_word(self, word, score):
        current_node = self.root
        for i in range(len(word)):
            next_node = current_node.children.get(word[i])
            if next_node is None:
                new_node =  Node()
                new_node.index = i + 1 
                current_node.children[word[i]] =new_node
                current_node = new_node
            else:
                current_node = next_node

        current_node.score = score
        current_node.is_word = True





class ScoreCounter:
    def __init__(self):
        self.trie = Trie()

    def process_dict(self, file_name):
        """
            This function creates a dict trie based on the given dictionary file.

            input: file_name (string), the file path of the dictionary file
            output: None 
        """
        with open(file_name) as f:
            for line in f:
                line = line.strip()
                i = len(line)-1 
                while not line[i].isspace() and i > 0:
                    i -=1
                    
                word = line[:i].rstrip()
                score = int(line[i+1:])  
                self.trie.add_word(word, score)
    
    def countScore(self,sentence):
        """
            This function is used for calculating the sentiment socre of the given string.
            The function examines the input string char by char and traverse the dict trie from the root, tries to find  
            a furthest node containing a score that matches the current char in the string.
            If there is no children node corresponding to the current char, it means there is no words to match 
            
            the function will restart to traverse from the root. 


            input: sentence (string)
            output: the score of the string (integer >= 0)
        """
        sentence = sentence.lower()
        score = 0 
        matched_index = -1
        matched_score = 0 
        current_index = 0 
        word_len = 0
        next_starting_index = -1 
        exception = "\"\'‘’“”?! ,.，。"
        current_node = self.trie.root
        while current_index < len(sentence):
            current_char = sentence[current_index]
            current_node = current_node.children.get(current_char) 

            if current_node is None :
                if matched_index != -1 and (matched_index-word_len <0 or sentence[matched_index-word_len] in exception) and ( sentence[matched_index+1] in exception ):
        
                    score += matched_score
                    current_index = matched_index + 1 
                    matched_index = -1

                else:
                    if next_starting_index != -1:
                        current_index = next_starting_index
                        next_starting_index = -1
                    else:
                        current_index +=1
                        while current_index < len(sentence) and (not (sentence[current_index-1] in exception)):
                            current_index +=1
                current_node = self.trie.root
            else:
                if current_node.is_word:
                    matched_index = current_index
                    matched_score = current_node.score
                    word_len = current_node.index
                
                    if current_index == len(sentence)-1 and (matched_index-word_len <0 or sentence[matched_index-word_len] in exception):
                        score += matched_score
                
                if current_char.isspace() and next_starting_index == -1:
                    next_starting_index = current_index + 1 

                current_index +=1
              
        return score


    

def main(argv):
   
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    #all processes need to process the grid & dict information 
    grids = parse_grid("melbGrid.json")
    counter = ScoreCounter()
    counter.process_dict("AFINN.txt")
    sta = process_tweets(argv[0], rank, size, grids, counter) # process tweets 

    if rank == 0: # master process
        #collect data from other processors and integrate the results
        for i in range(1, size):
            partial_sta = comm.recv(source=i)
            for key in partial_sta:
                prev = sta.get(key)
                partial = partial_sta.get(key)
                sta[key] = [prev[0] + partial[0], prev[1] + partial[1]] 
        
        #output final results
        print ("{:<8} {:<15} {:<15} ".format('Cell','#Total Tweets ','#Overal Sentiment Score'))
        for key in sorted(sta):
            value = sta.get(key)
            print ("{:<8} {:<15} {:<15} ".format(key,value[0], value[1]))
        

    else:#other processes
        comm.send(sta, dest=0) #send the statistical data to the root (rank 0 ) 
    




if __name__ == "__main__":  
    main(sys.argv[1:])
 

   






    
          
  

                


 
            
    
        
      

