from Pyro4 import expose
import time


class Solver:
    def __init__(self, workers=None, in_file_path=None, out_file_path=None):
        self.in_file_path = in_file_path
        self.out_file_path = out_file_path
        self.workers = workers

        self.text = ''
        self.word = ''
        print("Initialized")

    def solve(self):
        print("Job Started")
        print("Workers %d" % len(self.workers))

        (self.word, self.text) = self.read_input()

        start_time = time.time()

        # map
        mapped = []
        text_len = len(self.text)
        word_len = len(self.word)
        num_workers = len(self.workers)
        step = int(1. * text_len / num_workers)

        for i in range(0, num_workers):
            print("map %d" % i)

            start_at = step * i 
            end_at = min(step * (i+1) + word_len, text_len-1)
            
            mapped.append(self.workers[i].mymap(self.text, self.word, start_at, end_at))

        # reduce
        found_at = self.myreduce(mapped)

        end_time = time.time()

        # output
        if len(found_at) == 0:
            self.write_output([], end_time-start_time)
        self.write_output(found_at, end_time-start_time)

        print("Job Finished")

    @staticmethod
    @expose
    def mymap(text, word, start_at, end_at):
        working_text = text[start_at:end_at]
        found_at = Solver.search(working_text, word)
        found_at = [i+start_at for i in found_at]
        return found_at

    @staticmethod
    @expose
    def search(text, word):
        found_at = []

        M = len(word)
        N = len(text)
        i = 0
        j = 0
        p = 0    # hash value for word
        t = 0    # hash value for text
        q = 101  # prime number for hashing
        d = 256  # number of characters in the input alphabet
        h = 1
    
        # The value of h would be "pow(d, M-1)% q"
        for i in range(M-1):
            h = (h * d)% q
    
        # Calculate the hash value of pattern and first window of text
        for i in range(M):
            p = (d * p + ord(word[i]))% q
            t = (d * t + ord(text[i]))% q
    
        # Slide the pattern over text one by one
        for i in range(N-M + 1):
            # Check the hash values of current window of text and
            # pattern if the hash values match then only check
            # for characters one by one
            if p == t:
                # Check for characters one by one
                for j in range(M):
                    if text[i + j] != word[j]:
                        break
    
                j+= 1
                # if p == t and pat[0...M-1] = txt[i, i + 1, ...i + M-1]
                if j == M:
                    # print("Pattern found at index " + str(i))
                    found_at += [i]
    
            # Calculate hash value for next window of text: Remove
            # leading digit, add trailing digit
            if i < N-M:
                t = (d*(t-ord(text[i])*h) + ord(text[i + M]))% q
    
                # We might get negative values of t, converting it to
                # positive
                if t < 0:
                    t = t + q
        return found_at

    @staticmethod
    @expose
    def myreduce(mapped):
        res = []
        for x in mapped:
            res.extend(x.value)
            # res.extend(x)
        return list(set(res))

    def read_input(self):
        f = open(self.in_file_path, 'r')

        all_text = f.readlines()
        word = str(all_text[0].strip())
        text = ''.join(all_text[1:])
        
        f.close()
        return word, text
    
    def write_output(self, output, timer):
        f = open(self.out_file_path, 'w')

        if len(output) == 0:
            f.write("Sorry, we didn`t find this pattern in text :(")
            f.close()
            return

        patterns_count = str(len(output))
        f.write("We found ")
        f.write(patterns_count)
        f.write("patterns in text\n")

        f.write("With indices :\n")
        output.sort()
        f.write(str(output))

        f.write("\n\nTime spend to complete the job : ")
        f.write(str(timer))

        f.close()
        print("Output saved")


# if __name__ == '__main__':
#     master = Solver([Solver(), Solver(), Solver()],
#                     "./input_4.txt",
#                     "./output_1.txt")
#     master.solve()