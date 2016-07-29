package io.mindmaps.reasoner.internal.container;

import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.graql.api.query.Pattern;
import io.mindmaps.graql.api.query.Var;
import io.mindmaps.reasoner.internal.predicate.Atomic;
import io.mindmaps.reasoner.internal.predicate.AtomicFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static io.mindmaps.reasoner.internal.Utility.checkAtomsCompatible;

public class AtomConjunction {

    private final Set<Atomic> atomSet;

    public AtomConjunction(Pattern.Conjunction<Var.Admin> conj){
        this.atomSet = getAtomSet(conj);
    }
    private AtomConjunction(Set<Atomic> aSet){
        this.atomSet = aSet;
    }

    private Set<Atomic> getAtomSet(Pattern.Admin pattern) {
        Set<Atomic> atoms = new HashSet<>();

        Set<Var.Admin> vars = pattern.getVars();
        vars.forEach(var ->
        {
            Atomic atom = AtomicFactory.create(var);
            atoms.add(atom);
        });

        return atoms;
    }
    private Set<Atomic> getAtoms(){ return atomSet;}

    public boolean isEmpty(){ return atomSet.isEmpty();}
    public boolean contains(Atomic atom){ return atomSet.contains(atom);}

    public AtomConjunction add(Atomic atom){
        Set<Atomic> atoms = new HashSet<>();
        atomSet.forEach(a -> atoms.add(AtomicFactory.create(a)));
        atoms.add(atom);
        return new AtomConjunction(atoms);
    }
    public AtomConjunction remove(Atomic atom){
        Set<Atomic> atoms = new HashSet<>();
        atomSet.forEach(a -> atoms.add(AtomicFactory.create(a)));
        atoms.remove(atom);
        return new AtomConjunction(atoms);
    }
    public AtomConjunction conjunction(AtomConjunction conj, MindmapsTransaction graph) {

        boolean isCompatible = true;
        for (Atomic cAtom : conj.getAtoms() )
        {
            Iterator<Atomic> it = atomSet.iterator();
            while(it.hasNext() && isCompatible)
                isCompatible = checkAtomsCompatible(it.next(), cAtom, graph);
        }
        if (!isCompatible) return null;

        Set<Atomic> atoms = new HashSet<>();
        atomSet.forEach(a -> atoms.add(AtomicFactory.create(a)));
        conj.getAtoms().forEach(atom -> atoms.add(AtomicFactory.create(atom)) );
        return new AtomConjunction(atoms);
    }
    public Pattern.Conjunction<Var.Admin> getConjunction(){
        Set<Var.Admin> vars = new HashSet<>();
        atomSet.forEach(a -> vars.add(a.getPattern().asVar()));
        return Pattern.Admin.conjunction(vars);
    }
}
