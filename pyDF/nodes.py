#Sucuri - Minimalistic Python Dataflow Programming Library
#author: tiago@ime.uerj.br
from pydf import *
import bisect


class TaggedValue:
	def __init__(self, value, tag):
		self.value = value
		self.tag = tag
		self.request_task = True
	def __repr__(self):
		return "(%d, %s)" %(self.tag, self.value)

        def __cmp__(self, obj):
		if obj == None:
			return 1
                if not isinstance(obj, TaggedValue):
                        raise TypeError('can only compare TaggedValue with TaggedValue.')
                if self.tag > obj.tag:
                        return 1
                elif self.tag < obj.tag:
                        return -1
                else:
                        return 0



class Source(Node): #source class

        def __init__(self, it):
                self.it = it
                self.inport = []
                self.dsts = []
                self.tagcounter = 0

		self.affinity = None

        def run(self, args, workerid, operq):
                for line in self.it:
			result = self.f(line, args)

			tag = self.tagcounter
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
	def run(self, args, workerid, operq):
		if args[0] == None:                               	
        		opers = [Oper(workerid, None, None, None)]
        		self.sendops(opers, operq)
        		return 0

		tag = args[0].tag
		argvalues = [arg.value for arg in args]
		result = self.f(argvalues) 
		opers = self.create_oper(TaggedValue(result, tag), workerid, operq, tag)
		
		self.sendops(opers, operq)


class Feeder(Node):

        def __init__(self, value):
                self.value = value
                self.dsts = []
                self.inport = []
		self.affinity = None
		print "Setting feeder affinity"

        def f(self):
                print "Feedind %s" %self.value
                return self.value


class Serializer(Node):
	def __init__(self, f, inputn):
		Node.__init__(self, f, inputn)
		self.serial_buffer = []
		self.next_tag = 0
		self.arg_buffer = [[] for i in xrange(inputn)]
		self.f = f
		self.affinity = 0 #default affinity to Worker-0 (Serializer HAS to be pinned)

	def run(self, args, workerid, operq):
		if args[0] == None:
			opers = [Oper(workerid, None, None, None)]
			self.sendops(opers, operq)
			return 0
		
		#print "Got operand with tag %d (expecting %d) Worker %d" %(args[0].tag, self.next_tag, workerid)
		for (arg, argbuffer) in map(None, args, self.arg_buffer):
			bisect.insort(argbuffer, arg)
		if args[0].val.tag == self.next_tag:
			next = self.next_tag
			argbuffer = self.arg_buffer
			buffertag = argbuffer[0][0].val.tag
			while buffertag == next:
				args = [arg.pop(0) for arg in argbuffer]
				
				opers = self.create_oper(self.f([arg.val for arg in args]), workerid, operq)
				self.sendops(opers, operq)
				next += 1
				if len(argbuffer[0]) > 0:
					buffertag = argbuffer[0][0].val.tag
				else:
					buffertag = None

			self.next_tag = next


