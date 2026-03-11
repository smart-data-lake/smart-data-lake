/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package io.smartdatalake.workflow.dataframe.plainScala

import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericColumn}

import java.sql.Timestamp
import java.time.LocalDate

object ExpressionParser {

  def parse(expression: String)(implicit functions: DataFrameFunctions): GenericColumn = {
    try {
      val parser = new Parser(tokenize(expression), functions)
      val result = parser.parseExpression()
      parser.expect(TokenType.End, "end of expression")
      result
    } catch {
      case ex: ExpressionParserException => throw new IllegalArgumentException(s"${ex.message} at index ${ex.position} in expression: '$expression'", ex)
    }
  }

  private sealed trait TokenType
  private object TokenType {
    case object Number extends TokenType
    case object StringLiteral extends TokenType
    case object BooleanLiteral extends TokenType
    case object Identifier extends TokenType
    case object Plus extends TokenType
    case object Minus extends TokenType
    case object Multiply extends TokenType
    case object Divide extends TokenType
    case object Equal extends TokenType
    case object NotEqual extends TokenType
    case object LessThan extends TokenType
    case object GreaterThan extends TokenType
    case object LeftParen extends TokenType
    case object RightParen extends TokenType
    case object Comma extends TokenType
    case object Between extends TokenType
    case object And extends TokenType
    case object Or extends TokenType
    case object End extends TokenType
  }

  private case class Token(tokenType: TokenType, text: String, position: Int)

  private sealed trait ParsedValue {
    def toColumn(functions: DataFrameFunctions): GenericColumn
  }

  private case class ParsedColumn(column: GenericColumn) extends ParsedValue {
    override def toColumn(functions: DataFrameFunctions): GenericColumn = column
  }

  private case class ParsedLiteral(value: Any) extends ParsedValue {
    override def toColumn(functions: DataFrameFunctions): GenericColumn = functions.lit(value)
  }

  /**
   * Recursive-descent parser with explicit precedence levels:
   * or < and < comparison < additive < multiplicative < unary < primary.
   */
  private class Parser(tokens: Vector[Token], functions: DataFrameFunctions) {
    private var index: Int = 0

    /** Parse the full expression starting at the lowest precedence level. */
    def parseExpression(): GenericColumn = parseOr().toColumn(functions)

    /**
     * Consume one token of the expected type or fail with a position-aware error.
     */
    def expect(expected: TokenType, label: String): Unit = {
      if (current.tokenType != expected) {
        fail(s"Expected $label but found '${current.text}'")
      }
      index += 1
    }

    private def parseOr(): ParsedValue = {
      var left = parseAnd()
      while (current.tokenType == TokenType.Or) {
        val operator = current
        index += 1
        val right = parseAnd()
        left = buildBinary(left, operator, right)
      }
      left
    }

    private def parseAnd(): ParsedValue = {
      var left = parseComparison()
      while (current.tokenType == TokenType.And) {
        val operator = current
        index += 1
        val right = parseComparison()
        left = buildBinary(left, operator, right)
      }
      left
    }

    private def parseComparison(): ParsedValue = {
      var left = parseAdditive()
      var continue = true
      while (continue) {
        current.tokenType match {
          case TokenType.Equal | TokenType.NotEqual | TokenType.LessThan | TokenType.GreaterThan =>
            val operator = current
            index += 1
            val right = parseAdditive()
            left = buildBinary(left, operator, right)
          case TokenType.Between =>
            index += 1
            val lower = parseAdditive()
            expect(TokenType.And, "AND")
            val upper = parseAdditive()
            val valueCol = left.toColumn(functions)
            left = ParsedColumn(valueCol >= lower.toColumn(functions) and valueCol <= upper.toColumn(functions))
          case _ =>
            continue = false
        }
      }
      left
    }

    private def parseAdditive(): ParsedValue = {
      var left = parseMultiplicative()
      while (current.tokenType == TokenType.Plus || current.tokenType == TokenType.Minus) {
        val operator = current
        index += 1
        val right = parseMultiplicative()
        left = buildBinary(left, operator, right)
      }
      left
    }

    private def parseMultiplicative(): ParsedValue = {
      var left = parseUnary()
      while (current.tokenType == TokenType.Multiply || current.tokenType == TokenType.Divide) {
        val operator = current
        index += 1
        val right = parseUnary()
        left = buildBinary(left, operator, right)
      }
      left
    }

    private def parseUnary(): ParsedValue = {
      current.tokenType match {
        case TokenType.Plus =>
          index += 1
          parseUnary()
        case TokenType.Minus =>
          index += 1
          val right = parseUnary().toColumn(functions)
          ParsedColumn(functions.lit(0) - right)
        case _ =>
          parsePrimary()
      }
    }

    private def parsePrimary(): ParsedValue = {
      val token = current
      token.tokenType match {
        case TokenType.Number =>
          index += 1
          parseNumericLiteral(token)
        case TokenType.StringLiteral =>
          index += 1
          ParsedLiteral(token.text)
        case TokenType.BooleanLiteral =>
          index += 1
          ParsedLiteral(token.text.toBoolean)
        case TokenType.Identifier =>
          parseIdentifierOrSpecialSymbol()
        case TokenType.Multiply =>
          index += 1
          ParsedColumn(functions.col("*"))
        case TokenType.LeftParen =>
          index += 1
          val nestedExpr = parseOr()
          expect(TokenType.RightParen, ")")
          nestedExpr
        case _ =>
          fail(s"Unexpected token '${token.text}'")
      }
    }

    private def parseIdentifierOrSpecialSymbol(): ParsedValue = {
      val identifierToken = current
      val identifierText = identifierToken.text
      if (index + 1 < tokens.length && tokens(index + 1).tokenType == TokenType.LeftParen) {
        parseIdentifier()
      } else if ((identifierText.equalsIgnoreCase("timestamp") || identifierText.equalsIgnoreCase("date"))
        && index + 1 < tokens.length && tokens(index + 1).tokenType == TokenType.StringLiteral) {
        val literalToken = tokens(index + 1)
        index += 2
        parseTypedLiteral(identifierToken, literalToken)
      } else if (identifierText == "*") {
        index += 1
        ParsedColumn(functions.col("*"))
      } else {
        // interpret bare identifiers as column references
        index += 1
        ParsedColumn(functions.col(identifierText))
      }
    }

    private def parseTypedLiteral(typeToken: Token, literalToken: Token): ParsedValue = {
      if (typeToken.text.equalsIgnoreCase("timestamp")) {
        try {
          ParsedLiteral(Timestamp.valueOf(literalToken.text))
        } catch {
          case _: IllegalArgumentException => throw ExpressionParserException(s"Invalid timestamp literal '${literalToken.text}'", typeToken.position)
        }
      } else if (typeToken.text.equalsIgnoreCase("date")) {
        try {
          ParsedLiteral(Timestamp.valueOf(LocalDate.parse(literalToken.text).atStartOfDay()))
        } catch {
          case _: Exception => throw ExpressionParserException(s"Invalid date literal '${literalToken.text}'", typeToken.position)
        }
      } else throw ExpressionParserException(s"Unknown literal type '${typeToken.text}'", typeToken.position)
    }

    private def parseIdentifier(): ParsedValue = {
      val functionToken = current
      index += 1
      if (current.tokenType != TokenType.LeftParen) {
        fail(s"Column references or identifiers are not supported ('${functionToken.text}')")
      }

      index += 1 // consume '('
      val arguments = collection.mutable.ArrayBuffer.empty[ParsedValue]
      if (current.tokenType != TokenType.RightParen) {
        arguments += parseOr()
        while (current.tokenType == TokenType.Comma) {
          index += 1
          arguments += parseOr()
        }
      }
      expect(TokenType.RightParen, ")")

      ParsedColumn(invokeFunction(functionToken.text, arguments.toSeq, functionToken.position))
    }

    private def invokeFunction(functionName: String, args: Seq[ParsedValue], position: Int): GenericColumn = {
      val candidates = functions.getClass.getMethods
        .filter(_.getName.equalsIgnoreCase(functionName))
        .sortBy(_.getParameterCount)

      val resolved = candidates.view.flatMap(method => buildInvocationArguments(method, args).map(method -> _)).headOption
      resolved match {
        case Some((method, invocationArgs)) =>
          val result = method.invoke(functions, invocationArgs: _*)
          result match {
            case col: GenericColumn => col
            case _ => throw ExpressionParserException(s"Function '$functionName' does not return a column", position)
          }
        case None =>
          throw ExpressionParserException(s"No matching function found for '$functionName' with ${args.size} argument(s)", position)
      }
    }

    private def buildInvocationArguments(method: java.lang.reflect.Method, args: Seq[ParsedValue]): Option[Seq[AnyRef]] = {
      val paramTypes = method.getParameterTypes
      if (method.isVarArgs) {
        val fixedCount = paramTypes.length - 1
        if (args.size < fixedCount) return None

        val fixed = (0 until fixedCount).map(ix => convertArg(args(ix), paramTypes(ix)))
        if (fixed.contains(None)) return None

        val varArgType = paramTypes.last.getComponentType
        val remaining = args.drop(fixedCount).map(convertArg(_, varArgType))
        if (remaining.contains(None)) return None

        val varArgArray = java.lang.reflect.Array.newInstance(varArgType, remaining.size)
        remaining.zipWithIndex.foreach { case (valueOpt, ix) =>
          java.lang.reflect.Array.set(varArgArray, ix, valueOpt.get)
        }

        Some(fixed.flatten.map(_.asInstanceOf[AnyRef]) :+ varArgArray)
      } else if (paramTypes.length == 1 && classOf[scala.collection.Seq[_]].isAssignableFrom(paramTypes.head)) {
        // Scala repeated parameters are exposed as Seq in reflection for many methods.
        val seqArgs = args.map(_.toColumn(functions))
        Some(Seq(seqArgs))
      } else {
        if (paramTypes.length != args.length) return None
        val converted = args.zip(paramTypes).map { case (arg, paramType) => convertArg(arg, paramType) }
        if (converted.contains(None)) None else Some(converted.flatten.map(_.asInstanceOf[AnyRef]))
      }
    }

    private def convertArg(arg: ParsedValue, expectedType: Class[_]): Option[Any] = {
      if (classOf[GenericColumn].isAssignableFrom(expectedType)) Some(arg.toColumn(functions))
      else {
        arg match {
          case ParsedLiteral(value) => convertLiteral(value, expectedType)
          case ParsedColumn(_) => None
        }
      }
    }

    private def convertLiteral(value: Any, expectedType: Class[_]): Option[Any] = {
      expectedType match {
        case cls if cls == classOf[String] => value match {
          case s: String => Some(s)
          case _ => None
        }
        case cls if cls == classOf[Int] || cls == classOf[java.lang.Integer] => value match {
          case i: Int => Some(Int.box(i))
          case d: Double if d.isValidInt => Some(Int.box(d.toInt))
          case _ => None
        }
        case cls if cls == classOf[Double] || cls == classOf[java.lang.Double] => value match {
          case i: Int => Some(Double.box(i.toDouble))
          case d: Double => Some(Double.box(d))
          case _ => None
        }
        case cls if cls == classOf[Boolean] || cls == classOf[java.lang.Boolean] => value match {
          case b: Boolean => Some(Boolean.box(b))
          case _ => None
        }
        case cls if cls == classOf[Option[_]] => Some(Option(value))
        case cls if cls == classOf[Any] || cls == classOf[AnyRef] || cls == classOf[Object] => Some(value.asInstanceOf[AnyRef])
        case _ => None
      }
    }

    /**
     * Convert a numeric token to either Int or Double literal.
     * Values not fitting Int are parsed as Double.
     */
    private def parseNumericLiteral(token: Token): ParsedValue = {
      if (token.text.contains('.')) ParsedLiteral(token.text.toDouble)
      else {
        try ParsedLiteral(token.text.toInt)
        catch {
          case _: NumberFormatException => ParsedLiteral(token.text.toDouble)
        }
      }
    }

    private def current: Token = tokens(index)

    private def buildBinary(left: ParsedValue, operator: Token, right: ParsedValue): ParsedValue = {
      val leftCol = left.toColumn(functions)
      val rightCol = right.toColumn(functions)
      val result = operator.tokenType match {
        case TokenType.Plus => leftCol + rightCol
        case TokenType.Minus => leftCol - rightCol
        case TokenType.Multiply => leftCol * rightCol
        case TokenType.Divide => leftCol / rightCol
        case TokenType.Equal => leftCol === rightCol
        case TokenType.NotEqual => leftCol =!= rightCol
        case TokenType.LessThan => leftCol < rightCol
        case TokenType.GreaterThan => leftCol > rightCol
        case TokenType.And => leftCol.and(rightCol)
        case TokenType.Or => leftCol.or(rightCol)
        case _ => fail(s"Unsupported operator '${operator.text}'")
      }
      ParsedColumn(result)
    }

    private def fail(message: String): Nothing = {
      throw ExpressionParserException(message, current.position)
    }
  }

  /**
   * Tokenize the expression string into literals, operators and parentheses.
   * Whitespace is ignored.
   */
  private def tokenize(expression: String): Vector[Token] = {
    val tokens = Vector.newBuilder[Token]
    var index = 0

    def add(tokenType: TokenType, text: String, position: Int): Unit = {
      tokens += Token(tokenType, text, position)
    }

    while (index < expression.length) {
      expression.charAt(index) match {
        case c if c.isWhitespace =>
          index += 1

        case '(' =>
          add(TokenType.LeftParen, "(", index)
          index += 1
        case ')' =>
          add(TokenType.RightParen, ")", index)
          index += 1
        case '+' =>
          add(TokenType.Plus, "+", index)
          index += 1
        case '-' =>
          add(TokenType.Minus, "-", index)
          index += 1
        case '*' =>
          add(TokenType.Multiply, "*", index)
          index += 1
        case '/' =>
          add(TokenType.Divide, "/", index)
          index += 1
        case '=' if index + 1 < expression.length && expression.charAt(index + 1) == '=' =>
          add(TokenType.Equal, "==", index)
          index += 2
        case '=' =>
          add(TokenType.Equal, "=", index)
          index += 1
        case '!' if index + 1 < expression.length && expression.charAt(index + 1) == '=' =>
          add(TokenType.NotEqual, "!=", index)
          index += 2
        case '<' =>
          add(TokenType.LessThan, "<", index)
          index += 1
        case '>' =>
          add(TokenType.GreaterThan, ">", index)
          index += 1
        case ',' =>
          add(TokenType.Comma, ",", index)
          index += 1

        case '\'' =>
          val start = index
          val literal = new StringBuilder
          index += 1
          var closed = false
          while (index < expression.length && !closed) {
            val current = expression.charAt(index)
            if (current == '\'' && index + 1 < expression.length && expression.charAt(index + 1) == '\'') {
              literal.append('\'')
              index += 2
            } else if (current == '\'') {
              closed = true
              index += 1
            } else {
              literal.append(current)
              index += 1
            }
          }
          if (!closed) {
            throw ExpressionParserException(s"Unterminated string literal", start)
          }
          add(TokenType.StringLiteral, literal.toString(), start)

        case c if c.isDigit =>
          val start = index
          var seenDot = false
          while (index < expression.length && {
            val ch = expression.charAt(index)
            if (ch == '.') {
              if (seenDot) false else {
                seenDot = true
                true
              }
            } else ch.isDigit
          }) {
            index += 1
          }
          add(TokenType.Number, expression.substring(start, index), start)

        case c if c.isLetter || c == '_' =>
          val start = index
          var seenDot = false
          while (index < expression.length && {
            val ch = expression.charAt(index)
            ch.isLetterOrDigit || ch == '_' || (!seenDot && ch == '.')
          }) {
            if (expression.charAt(index) == '.') {
              seenDot = true
            }
            index += 1
          }
          val text = expression.substring(start, index)
          if (text.equalsIgnoreCase("true") || text.equalsIgnoreCase("false")) {
            add(TokenType.BooleanLiteral, text.toLowerCase, start)
          } else if (text.equalsIgnoreCase("between")) {
            add(TokenType.Between, text, start)
          } else if (text.equalsIgnoreCase("and")) {
            add(TokenType.And, text, start)
          } else if (text.equalsIgnoreCase("or")) {
            add(TokenType.Or, text, start)
          } else {
            add(TokenType.Identifier, text, start)
          }

        case other =>
          throw ExpressionParserException(s"Unexpected character '$other'", index)
      }
    }

    add(TokenType.End, "<end>", expression.length)
    tokens.result()
  }

}

case class ExpressionParserException(message: String, position: Int) extends Exception(s"$message at index $position")