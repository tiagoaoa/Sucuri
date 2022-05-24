#Sucuri - Minimalistic Python Dataflow Programming Library
#author: tiago@ime.uerj.br
from pyDF.pydf import *
import bisect


class TaggedValue:
    def __init__(self, value, tag):
        self.value = value
        self.tag = tag
        self.request_task = True
    def __repr__(self):
        return "TaggedValue: ({}, {})".format(self.tag, self.value)

    def __test_obj__(self, obj):
        if not isinstance(obj, TaggedValue):
            raise TypeError('can only compare TaggedValue with TaggedValue, but got {} and {}'.format(self, obj))


    def __cmp__(self, obj):
        if obj == None:
            return 1
        self.__test_obj__(obj)
        return self.tag - obj.tag

    def __lt__(self, obj):
        return self.__cmp__(obj) < 0
    def __gt__(self, obj):
        return self.__cmp__(obj) > 0
    
    def __eq__(self, obj):
        return self.__cmp__(obj) == 0
    
    def __le__(self, obj):
        return self.__cmp__(obj) <= 0
    def __ge__(self, obj):
        return self.__cmp__(obj) >= 0
    def __ne__(self, obj):
        return self.__cmp__(obj) != 0






class Source(Node): #source class

    def __init__(self, it):
        self.it = it
        self.inport = []
        self.dsts = []
        self.tagcounter = 0

        self.affinity = None

    def run(self, args, workerid, operq):
        for line in self.it:
            tag = self.tagcounter
            
            result = self.f(line, args)
            #print("Creating oper {} A".format(result))
            opers = self.create_oper(TaggedValue(result, tag), workerid, operq)
            for oper in opers:
                oper.request_task = False
                self.sendops(opers, operq)
            self.tagcounter += 1
            opers = [Oper(workerid, None, None, None)] #sinalize eof and request a task
            self.sendops(opers, operq)
    def f(self, line, args):
        #default source operation
        return line


class FlipFlop(Node):
    def __init__(self, f):
        self.f = f
        self.inport = [[],[]]
        self.dsts = []
        self.affinity = None

    def run(self, args, workerid, operq):
        opers = self.create_oper(self.f([a.val for a in args]), workerid, operq)

        if opers[0].val == False:
            opers = [Oper(workerid, None, None, None)]
        self.sendops(opers, operq)


class FilterTagged(Node): #produce operands in the form of TaggedValue, with the same tag as the input
    def __init__(self, f, inputn):
        Node.__init__(self, f, inputn)
        self.match_dict = {}
        #self.arg_buffer = [[] for i in range(inputn)]
        self.inputn = inputn
        self.f = f
        #print("Selfdst {}".format(self.dsts))

    def insert_op(self, dstport, oper):
        #print("opertype {}".format(oper.val))
        tag = oper.val.tag
        value = oper.val.value

        #print("match dict {}".format(self.match_dict))
        if oper.val.tag in self.match_dict:
            #   print("Appending")
            self.match_dict[tag].append(value)
        else:
            self.match_dict[tag] = [value]

    #TODO: implement special match() method
    def match(self):
        match_d = self.match_dict
        tags = [tag for tag in match_d if len(match_d[tag]) == len(self.inport)]
        #print("Receiving args {}".format(tags)
        if len(tags) > 0:
            tag = tags[0]
        else:
            return None
        args = [TaggedValue(val, tag) for val in self.match_dict.pop(tag)]
        return args






    def run(self, args, workerid, operq):
        argvalues = [arg.value for arg in args]
        tag = args[0].tag
        #print("Args {}".format(args))



        result = self.f(argvalues)
        if result != None:
            result = TaggedValue(result, tag)
        #else:
        #    print("Creating just request {}".format(self.dsts))
        opers = self.create_oper(result, workerid, operq)
        self.sendops(opers, operq)


class Feeder(Node):
    def __init__(self, value):
        self.value = value
        self.dsts = []
        self.inport = []
        self.affinity = None
        print("Setting feeder affinity")

    def f(self):
        #print "Feeding %s" %self.value
        return self.value


class Serializer(Node):
    def __init__(self, f, inputn):
        Node.__init__(self, f, inputn)
        self.serial_buffer = []
        self.next_tag = 0
        self.arg_buffer = [[] for i in range(inputn)]
        self.f = f
        self.affinity = [0] #default affinity to Worker-0 (Serializer HAS to be pinned)

    def run(self, args, workerid, operq):
        if args[0] == None:
            #TODO: check if this if-statement is still necessary.
            opers = [Oper(workerid, None, None, None)]
            self.sendops(opers, operq)
            return 0

        for (arg, argbuffer) in zip(args, self.arg_buffer):
            bisect.insort(argbuffer, arg.val)
            #print "Argbuffer %s" %argbuffer
        #print "Got operand with tag %s (expecting %d) %s Worker %d" %([arg.val for arg in args], self.next_tag, [arg.val for arg in argbuffer], workerid)
        if args[0].val.tag == self.next_tag:
            next = self.next_tag
            argbuffer = self.arg_buffer
            buffertag = argbuffer[0][0].tag
            while buffertag == next:
                args = [arg.pop(0) for arg in argbuffer]
                #print("Sending oper with tag {}".format(args[0].tag))	
                opers = self.create_oper(self.f([arg.value for arg in args]), workerid, operq)
                self.sendops(opers, operq)
                next += 1
                if len(argbuffer[0]) > 0:
                    buffertag = argbuffer[0][0].tag
                else:
                    buffertag = None

            self.next_tag = next


