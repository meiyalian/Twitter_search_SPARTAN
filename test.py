from mpi4py import MPI
import json
import sys



def convert_to_json(line_number, data ):
    if line_number > 0: #ignore the first line of file 
        data = data.strip()
        if data[-2] == "]":
            data = data[:-2]
        else:
            if data[-1] == ",":
                data = data[:-1]
        return data



def process_tweets(data_file, rank, no_of_process, grids_arr, score_counter ):
    statistics = dict()
    for grid in grids_arr:
        statistics[grid["id"]] = [0,0] # each cell in dict stores [number_of_tweets, overall_score ]

    with open(data_file) as f:
        for i, line in enumerate(f):
            if i % no_of_process == rank:
                try:
                    tweet = convert_to_json(i, line)
                    if tweet is not None: 
                        tweet = json.loads(tweet)
                        location = locate_coord(grids_arr, tweet["value"]["geometry"]["coordinates"])
                        if location is not None:
                            score = score_counter.countScore(tweet["value"]["properties"]["text"])
                            previous = statistics.get(location)
                            statistics[location] = [previous[0]+1, previous[1]+score ]
            
                except ValueError:
                    print("line number: ", i, ", \nMalfomred json: " ,line)

        return statistics



def parse_grid(fname):
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
        sentence = sentence.lower()
        score = 0 
        matched_index = -1
        matched_score = 0 
        current_index = 0 
        word_len = 0
        # has_match = False
        exception = "\"\'‘’“”?! ,.，。"
        current_node = self.trie.root
        while current_index < len(sentence):
            current_char = current_node.children.get(sentence[current_index]) 
            if current_char is None :
                if matched_index != -1 and (matched_index-word_len <0 or sentence[matched_index-word_len] in exception) and ( sentence[matched_index+1] in exception ):
                    #print(sentence[matched_index-word_len+1: matched_index+1])
                    score += matched_score
                    current_index = matched_index + 1 
                    matched_index = -1

                else:
                    if sentence[current_index-1] in exception and current_node!= self.trie.root:
                        pass
                    else:
                        current_index +=1
                        while current_index < len(sentence) and (not (sentence[current_index-1] in exception)):
                            current_index +=1
                current_node = self.trie.root
            else:
                if current_char.is_word:
                    matched_index = current_index
                    matched_score = current_char.score
                    word_len = current_char.index
                
                    if current_index == len(sentence)-1 and (matched_index-word_len <0 or sentence[matched_index-word_len] in exception):
                        #print(sentence[matched_index-word_len+1: matched_index+1])
                        score += matched_score
                
                current_index +=1
                current_node = current_char
        return score



def main(argv):
   
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    # runtime = MPI.Wtime()
    #all processes need to process the grid & dict information 
    grids = parse_grid("melbGrid.json")
    counter = ScoreCounter()
    counter.process_dict("AFINN.txt")
    sta = process_tweets(argv[0], rank, size, grids, counter)

    if rank == 0: # master process
        # print("this is rank: ", rank )
        for i in range(1, size):
            partial_sta = comm.recv(source=i)
            for key in partial_sta:
                prev = sta.get(key)
                partial = partial_sta.get(key)
                sta[key] = [prev[0] + partial[0], prev[1] + partial[1]] 
        
        # runtime = MPI.Wtime() - runtime
        #print final results
        print("Cell      ","#Total Tweets      ", "#Overal Sentiment Score")
        for key in sorted(sta):
            value = sta.get(key)
            print(key, "            ", value[0], "                             ",  value[1])
        
        # print("Total run time: ", runtime, " seconds.")

    else:
        # print("this is rank: ", rank )
        comm.send(sta, dest=0)
    




if __name__ == "__main__":  
    # main(sys.argv[1:])
    grids = parse_grid("melbGrid.json")
    for each in grids:
        print(each)






    
          
  

                


 
            
    
        
      

