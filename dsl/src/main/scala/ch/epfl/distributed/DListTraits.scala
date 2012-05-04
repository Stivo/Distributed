package ch.epfl.distributed

import scala.virtualization.lms.common._
import scala.virtualization.lms.internal._

trait DListProgram extends DListOps 
	with ScalaOpsPkg 
	with LiftScala

trait DListProgramExp extends DListOpsExp
	with ScalaOpsPkgExp
	with FatExpressions with LoopsFatExp with IfThenElseFatExp
    with BlockExp with Effects with EffectExp

trait BaseCodeGenerator extends ScalaCodeGenPkg 
	with SimplifyTransform with GenericFatCodegen with LoopFusionOpt
    with FatScheduling with BlockTraversal 
    //with LivenessOpt
	{ val IR: DListProgramExp }

trait PrinterGenerator extends BaseCodeGenerator 
	with PrinterGenDList
//	with ScalaGenArrayOps with ScalaGenPrimitiveOps with ScalaGenStringOps
//	with ScalaGenFunctions with ScalaGenWhile with ScalaGenVariables
	{ val IR: DListProgramExp }