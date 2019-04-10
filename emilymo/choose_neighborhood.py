import dml
from pymongo import MongoClient
import prov
import datetime
import uuid
from z3 import *

class choose_neighborhood(dml.Algorithm):
    contributor = 'emilymo'
    reads = ['a.bnd', 'a.info']
    writes = ['a.sols', 'a.selected']
    
    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()
        
        client = dml.pymongo.MongoClient() 
        a = client.a
        
        #####################
        
        def vname(let, num):
            return let + str(num)
        
        c = z3.Solver()
        
        var = {}
        
        vcom = {} # number of ___ regardless of inclusion
        vpss = {}
        vnpss = {}
        vcoh = {}
        vcon = {}
        vrec = {}
        
        ccom = {} # if included, 1 if satisfies constraint on ___ , 0 otherwise 
        cpss = {}
        cnpss = {}
        ccoh = {}
        ccon = {}
        crec = {}
        
        for i in range(0, len([r for r in a['info'].find()])):
            nb = [r for r in a['info'].find()][i] # row in info
            n = i+1
            
            # create variable for all neighborhoods' in / out status
            incl = vname('x', n)
            var[incl] = Real(incl)
            c.add(Or(var[incl] == 0, var[incl] == 1)) # must be 0 or 1
            
            # create variable for whether constraint on each variable is satisfied regardless of other constraints
            if nb['comms'] <= 2:
                vcom[vname('commsn', n)] = 1
            else:
                vcom[vname('commsn', n)] = 0
            if nb['pss'] >=2:
                vpss[vname('pssn', n)] = 1
            else:
                vpss[vname('pssn', n)] = 0
            if nb['npss'] >= 1: 
                vnpss[vname('npssn', n)] = 1
            else:
                vnpss[vname('npssn', n)] = 0
                
            if nb['cohesion'] <= [r for r in a['bnd'].find()][0]['cohesion']:
                vcoh[vname('cohesionn', n)] = 1
            else:
                vcoh[vname('cohesionn', n)] = 0
                
            if nb['control'] <= [r for r in a['bnd'].find()][0]['control']:
                vcon[vname('controln', n)] = 1
            else:
                vcon[vname('controln', n)] = 0
                
            if nb['reciprocal'] <= [r for r in a['bnd'].find()][0]['reciprocal']:
                vrec[vname('reciprocaln', n)] = 1
            else:
                vrec[vname('reciprocaln', n)] = 0
            
            # create variable with respect to constraint for all attributes
            commsc = vname('commsc', n) 
            ccom[commsc] = Real(commsc)  # this dict will have incl status * constraint satisfied status
            c.add(ccom[commsc] == var[incl] * vcom[vname('commsn', n)])
            
            pssc = vname('pssc', n) 
            cpss[pssc] = Real(pssc)  
            c.add(cpss[pssc] == var[incl] * vpss[vname('pssn', n)])
            
            npssc = vname('npssc', n) 
            cnpss[npssc] = Real(npssc)  
            c.add(cnpss[npssc] == var[incl] * vnpss[vname('npssn', n)])
            
            cohesionc = vname('cohesionc', n) 
            ccoh[cohesionc] = Real(cohesionc)  
            c.add(ccoh[cohesionc] == var[incl] * vcoh[vname('cohesionn', n)])
            
            controlc = vname('controlc', n) 
            ccon[controlc] = Real(controlc)  
            c.add(ccon[controlc] == var[incl] * vcon[vname('controln', n)])
            
            reciprocalc = vname('reciprocalc', n) 
            crec[reciprocalc] = Real(reciprocalc)  
            c.add(crec[reciprocalc] == var[incl] * vrec[vname('reciprocaln', n)])
            
        c.add(sum(var.values()) == 1) # must select just one nbhd
        
        # add more constraints, prioritized by the order in which they are tried
        pushbool = True # push for next iteration
        passes = []
        params = [ccom.values(), ccoh.values(), ccon.values(),  crec.values(), cpss.values(), cnpss.values()]
        paramnames = ['comms', 'coh', 'con', 'rec', 'pss', 'npss'] # order of priority
        log = [] # log of what constraints were added and rejected
        for i in range(6):
            cond = params[i]
            nm = paramnames[i]
            if pushbool == True:
                c.push() # open up to new constraints
            c.add(sum(cond) == 1) # add constraint to see if sat
            if c.check() == sat:
                passes.append(cond) # if sat, add satisfactory constraints to passes
                print ('added ' + nm + ' constraint')
                log.append(('added ' + nm + ' constraint'))
                pushbool = False # then don't do a push for next iteration, keep adding to current push
            elif c.check() == unsat:
                print ('rejected ' + nm + ' constraint')
                log.append(('rejected ' + nm + ' constraint'))
                c.pop() # if unsat, pop off all tried constraints (includes passed and non-passed. will add passed back on next)
                for i in passes:
                    c.add(sum(i) == 1) # add all passed constraints permanently
                    passes = [] # empty passes for next batch
                pushbool = True # signal to open a new push for next iteration
                
        # if loop ends and pushbool is false, it means that the current tried constraints haven't been permanently added yet. pop everything off and add the passed ones permanently. if loop ends and pushbool is true, that means we already added the last batch of passed constraints.
        if pushbool == False: 
            c.pop()
            for i in passes: # permanently add passed constraints
                c.add(sum(i) == 1)
            
        print(c.check())
        print(c.model())
        
        # are there other solutions? if so, add to list of solutions
        indices = [] 
        sols = [] # list of satisfactory models 
        c.push()
        while c.check() == sat:
            tempsol = c.model() # last satisfactory model tested
            sols.append(tempsol) # add current model to list of satisfactory models
            modstr = str(tempsol).replace('[', '').replace(']', '').split(',\n ')
            modstr = [r.split(' = ') for r in modstr]
            passed = [r[0] for r in modstr if r[0][0] == 'x' and r[1] == '1'][0]  # get variable name indicating which neighborhood was selected
            print(passed + " = 1")
            indices.append(int(passed[1:])) # record index of current satisfying neighborhood so that can add new constraint against it  
            c.add(var[passed] == 0) # add constraint...
            if c.check() == sat: # check to see if any satisfying neighorhoods remaining after adding constraint, if so:
                print("model: " + c.model()) 
        c.pop()
        
        selected = [[e for e in a['info'].find()][i-1] for i in indices] # have to subtract 1 because the variable indices start counting at 1
        a['selected'].insert_many(selected) 
        
        sol = []
        for s in sols:
            d = str(s).replace('[', '').replace(']', '').split(',\n ')
            d = dict([r.split(' = ') for r in d])
            sol.append(d)
        
        a['sols'].insert_many(sol)
        
        a.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}
    
    
    
    
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/') 
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') 
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        
        this_script = doc.agent('alg:emilymo#choose_neighborhood', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent']})
        choose_neighborhood = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        info = doc.entity('dat:emilymo#info', {prov.model.PROV_LABEL:'Community Centers, Schools, and Survey Data by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        bnd = doc.entity('dat:emilymo#bnd', {prov.model.PROV_LABEL:'Mean Survey Values Across All Neighborhoods', prov.model.PROV_TYPE:'ont:DataSet'})
        sols = doc.entity('dat:emilymo#sols', {prov.model.PROV_LABEL:'Satisfactory Models', prov.model.PROV_TYPE:'ont:DataSet'})
        selected = doc.entity('dat:emilymo#selected', {prov.model.PROV_LABEL:'Info For Selected Neighborhoods', prov.model.PROV_TYPE:'ont:DataSet'})
                          
        doc.wasAssociatedWith(choose_neighborhood, this_script)
        doc.wasAttributedTo(sols, this_script)
        doc.wasGeneratedBy(sols, choose_neighborhood)
        doc.wasAttributedTo(selected, this_script)
        doc.wasGeneratedBy(selected, choose_neighborhood)
        doc.used(choose_neighborhood, info, other_attributes={prov.model.PROV_TYPE:'ont:Computation'})
        doc.used(choose_neighborhood, bnd, other_attributes={prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasDerivedFrom(sols, info)
        doc.wasDerivedFrom(sols, bnd)
        doc.wasDerivedFrom(selected, info)

        
        return doc