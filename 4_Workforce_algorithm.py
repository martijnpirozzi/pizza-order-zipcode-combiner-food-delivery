"""
Final Assignment Data Analysis and Programming for Operations Management
Code developed by:      Martijn de Jonge - Pirozzi
Student number:         S4147286

Goal of the code:
Determine most optimal work force based on historical demand
"""
# Model completely built by Martijn de Jonge S4147286

%reset -f

from gurobipy import Model, GRB

m = Model("assignment")

# historical demands from the year 2018
# demands per hour per day of the week normal days. e.g. D1 = Monday
D1 = [11,2,2,15,14,4,4,13,15,32,40,26,6,2,1]
D2 = [10,2,2,13,11,3,3,11,13,29,34,22,5,2,1]
D3 = [11,2,2,14,13,4,4,13,14,32,38,26,6,2,1]
D4 = [21,4,3,27,26,7,7,24,27,64,77,49,10,4,1]
D5 = [44,8,6,56,51,15,16,53,59,132,158,104,22,8,1]
D6 = [29,5,4,38,34,10,10,34,39,87,104,69,14,5,1]
D7 = [22,4,3,30,27,8,8,27,28,68,85,48,11,4,1]

# demands per hour for Dutch National Holidays
NYE = [23, 3, 2, 40, 25, 6, 10, 27, 34, 73, 94, 65, 13, 4, 1]               #New Years Day
GFD = [50, 10, 6, 67, 66, 12, 17, 68, 52, 159, 157, 120, 28, 9, 0]          #Good Friday
EAS = [31, 5, 5, 47, 40, 10, 9, 42, 37, 88, 104, 69, 12, 6, 0]              #Easter (avg of both days)
KID = [112, 17, 14, 115, 123, 26, 27, 98, 132, 339, 417, 232, 38, 17, 0]    #Kings Day
LID = [59, 9, 15, 113, 87, 18, 31, 94, 79, 224, 228, 170, 29, 9, 1]         #Liberation Day
ASD = [28, 4, 3, 34, 27, 8, 10, 31, 42, 79, 74, 54, 12, 4, 1]               #Ascension Day
PTC = [16, 4, 2, 25, 28, 6, 6, 20, 26, 68, 72, 41, 9, 4, 0]                 #Pentecost (avg of both days)
CHR = [13, 5, 4, 34, 20, 9, 8, 29, 32, 60, 87, 58, 9, 4, 1]                 #Christmas (avg of both days)

# Set demand for the day you want to optimize
D = CHR

# possible starts per day x and y
# x = 8 hour shift. Can not start later then 24-8=16pm
# y = 4 hour shift. Can not start later then 24-4=20pm
x1 = m.addVar(lb=0, name="x1")
x2 = m.addVar(lb=0, name="x2")
x3 = m.addVar(lb=0, name="x3")
x4 = m.addVar(lb=0, name="x4")
x5 = m.addVar(lb=0, name="x5")
x6 = m.addVar(lb=0, name="x6")
x7 = m.addVar(lb=0, name="x7")
x8 = m.addVar(lb=0, name="x8")

y1 = m.addVar(lb=0, name="y1")
y2 = m.addVar(lb=0, name="y2")
y3 = m.addVar(lb=0, name="y3")
y4 = m.addVar(lb=0, name="y4")
y5 = m.addVar(lb=0, name="y5")
y6 = m.addVar(lb=0, name="y6")
y7 = m.addVar(lb=0, name="y7")
y8 = m.addVar(lb=0, name="y8")
y9 = m.addVar(lb=0, name="y9")
y10 = m.addVar(lb=0, name="y10")
y11 = m.addVar(lb=0, name="y11")
y12 = m.addVar(lb=0, name="y12")


# Constraints for demand. Shifting each hour
m.addConstr(x1 + y1 >=  D[0]) #9-10am
m.addConstr(x1 + x2 + y1 + y2 >= D[1]) #10-11am
m.addConstr(x1 + x2 + x3 + y1 + y2 + y3 >= D[2]) #11-12am
m.addConstr(x1 + x2 + x3 + x4 + y1 + y2 + y3 + y4 >= D[3]) #12am-13pm - 4
m.addConstr(x1 + x2 + x3 + x4 + x5 + y2 + y3 + y4 + y5 >= D[4]) #13-14pm
m.addConstr(x1 + x2 + x3 + x4 + x5 + x6 + y3 + y4 + y5 + y6 >= D[5]) #14-15pm
m.addConstr(x1 + x2 + x3 + x4 + x5 + x6 + x7 + y4 + y5 + y6 + y7 >= D[6]) #15-16pm - latest start 8 hour
m.addConstr(x1 + x2 + x3 + x4 + x5 + x6 + x7 + x8 + y5 + y6 + y7 + y8 >= D[7]) #16-17pm - 8
m.addConstr(x2 + x3 + x4 + x5 + x6 + x7 + x8 +  y6 + y7 + y8 + y9 >= D[8]) #17-18pm
m.addConstr(x3 + x4 + x5 + x6 + x7 + x8 +  y7 + y8 + y9 + y10 >= D[9]) #18-19pm
m.addConstr(x4 + x5 + x6 + x7 + x8 +  y8 + y9 + y10 + y11 >= D[10]) #19-20pm
m.addConstr(x5 + x6 + x7 + x8 + y9 + y10 + y11 + y12 >= D[11]) #20-21pm
m.addConstr(x6 + x7 + x8 + y10 + y11 + y12 >= D[12]) #21-22pm
m.addConstr(x7 + x8 +  y11 + y12 >= D[13]) #22-23pm
m.addConstr(x8 + y12 >= D[14]) #23-24pm

x = x1 + x2 + x3 + x4 + x5 + x6 + x7 + x8
y = y1 + y2 + y3 + y4 + y5 + y6 + y7 + y8 + y9 + y10 + y11

m.setObjective(9 * 8 * x + 10 * 4 * y, GRB.MINIMIZE)

m.optimize()

for v in m.getVars():
    print("%s %g" % (v.varName, v.x))

print("Obj: %g" % m.objVal)
