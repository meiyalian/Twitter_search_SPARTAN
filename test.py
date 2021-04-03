# from mpi4py import MPI
import json
import re

# comm = MPI.COMM_WORLD
# size = MPI_comm.Get_size()



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
    i = 0
    j = -1
    while i < len(grids):
        if coord[0] >= grids[i]["xmin"] and coord[0]<= grids[i]["xmax"]:
            j = i 
            while j < len(grids) and grids[j]["xmin"] == grids[i]["xmin"]:
                j+=1 
            break
            
        i +=1
    
    if j == -1: #outside of range 
        return ""
    
    for each in grids[i:j]:
        if coord[1] >= each["ymin"] and coord[1] <= each["ymax"] :
            return each["id"]
    return ""





class Node:
    def __init__(self, value):
        self.val = value
        self.children = dict()
        self.index = 0 
        self.score = 0
        self.is_word = False
    


class Trie:
    def __init__(self):
        self.root = Node(None)
    
    def add_word(self, word, score):
        current_node = self.root
        for i in range(len(word)):
            next_node = current_node.children.get(word[i])
            if next_node is None:
                new_node =  Node(word[i])
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
        exception = "\"\'?! ,."
        current_node = self.trie.root
        while current_index < len(sentence):
            current_char = current_node.children.get(sentence[current_index]) 
            if current_char is None :
                if matched_index != -1 and (matched_index-word_len <0 or sentence[matched_index-word_len] in exception) and ( sentence[matched_index+1] in exception ):
                    score += matched_score
                    current_index = matched_index + 1 
                    matched_index = -1

                else:
                    current_index +=1
                    while current_index < len(sentence) and (not sentence[current_index-1] in exception):
                        current_index +=1
                current_node = self.trie.root
            else:
                if current_char.is_word:
                    matched_index = current_index
                    matched_score = current_char.score
                    word_len = current_char.index
                
                    if current_index == len(sentence)-1 and (matched_index-word_len <0 or sentence[matched_index-word_len] in exception):
                        score += matched_score
                
                current_index +=1
                current_node = current_char
        return score








    

            
        

if __name__ == "__main__":  
    # grids = parse_grid("melbGrid.json")
    # print([g["id"] for g in grids])
    # print(locate_coord(grids, [145.3, -37.8]))
    # t = Trie()
    # t.add_word("apple", 3)
    # t.add_word("appl", 4)
    # print(t.root.children["a"].children["p"].children["p"].children["l"].children["e"].index)

    # find_score = re.compile(r'[-+]?[0-9]+')  # 查找正负数字
    # find_word = re.compile('[^-\d\t\n]+')  # 查找词
    # with open("AFINN.txt") as f:
    #     for line in f:
    #         word = find_word.findall(line)
    #         score = find_score.findall(line)
    #         print(word[0], ": ", score[0])

    counter = ScoreCounter()
    counter.process_dict("AFINN.txt")
    # print(counter.countScore("aBandoabandon"))
    # print(counter.countScore("can't stand abandon."))
    print(counter.countScore("can't stand abandon.....@!abandon."))

 
            
    
        
      

