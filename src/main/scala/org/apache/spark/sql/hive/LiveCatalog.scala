package org.apache.spark.sql.hive

import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.catalog.{CatalogExtension, CatalogPlugin, Identifier, NamespaceChange, SupportsNamespaces, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class UnityCatalog[T <: TableCatalog with SupportsNamespaces] extends CatalogExtension
  with SupportsNamespaces{
  override def setDelegateCatalog(delegate: CatalogPlugin): Unit = ???

  override def listNamespaces(): Array[Array[String]] = ???

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = ???

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = ???

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = ???

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = ???

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = ???

  override def listFunctions(namespace: Array[String]): Array[Identifier] = ???

  override def loadFunction(ident: Identifier): UnboundFunction = ???

  override def listTables(namespace: Array[String]): Array[Identifier] = ???

  override def loadTable(ident: Identifier): Table = ???

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = ???

  override def alterTable(ident: Identifier, changes: TableChange*): Table = ???

  override def dropTable(ident: Identifier): Boolean = ???

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = ???

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = ???

  override def name(): String = ???
}
