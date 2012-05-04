package ch.epfl.distributed

import scala.virtualization.lms.common._
import scala.virtualization.lms.internal._

trait DListProgram extends DListOps 
	with ScalaOpsPkg 
	with LiftScala
	with MoreIterableOps with StringAndNumberOps with DateOps 

trait DListOpsExpBase extends DListOps
	with ScalaOpsPkgExp
	with FatExpressions with BlockExp with Effects with EffectExp
	with MoreIterableOpsExp
	with StringPatternOpsExp with DateOpsExp
	with StringAndNumberOpsExp
	with IfThenElseFatExp with LoopsFatExp
	with StructTupleOpsExp
	
trait DListProgramExp extends DListOpsExp
	with ScalaOpsPkgExp
	with FatExpressions with LoopsFatExp with IfThenElseFatExp
    

trait BaseCodeGenerator extends ScalaCodeGenPkg 
	with SimplifyTransform with GenericFatCodegen with LoopFusionOpt
    with FatScheduling with BlockTraversal 
    with StringAndNumberOpsCodeGen
    //with LivenessOpt
	{ val IR: DListProgramExp }

/*
 trait PrinterGenerator extends BaseCodeGenerator 
	with PrinterGenDList
//	with ScalaGenArrayOps with ScalaGenPrimitiveOps with ScalaGenStringOps
//	with ScalaGenFunctions with ScalaGenWhile with ScalaGenVariables
	{ val IR: DListProgramExp }
*/