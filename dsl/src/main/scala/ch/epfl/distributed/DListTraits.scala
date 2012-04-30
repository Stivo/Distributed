package ch.epfl.distributed

import scala.virtualization.lms.common._

trait DListProgram extends DListOps 
	with ScalaOpsPkg 
	with LiftScala

trait DListProgramExp extends DListOpsExp
	with ScalaOpsPkgExp
//	with ArrayOpsExp with PrimitiveOpsExp with StringOpsExp
//	with FunctionsExp with WhileExp with VariablesExp

trait PrinterGenerator extends PrinterGenDList 
	with ScalaCodeGenPkg
//	with ScalaGenArrayOps with ScalaGenPrimitiveOps with ScalaGenStringOps
//	with ScalaGenFunctions with ScalaGenWhile with ScalaGenVariables
	{ val IR: DListProgramExp }