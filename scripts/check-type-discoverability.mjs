import { existsSync, statSync } from "node:fs";
import { dirname, isAbsolute, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";

function isPathWithin(directory, file) {
	const relativePath = relative(directory, file);
	return relativePath !== ".." && !relativePath.startsWith(`..${sep}`) && !isAbsolute(relativePath);
}

function formatConfigDiagnostics(diagnostics) {
	return ts.formatDiagnostics(diagnostics, {
		getCanonicalFileName: (fileName) => fileName,
		getCurrentDirectory: () => process.cwd(),
		getNewLine: () => "\n",
	});
}

function getContractSymbol(program, checker, type) {
	const symbol = type.aliasSymbol ?? type.getSymbol();
	if (!symbol || symbol.name.startsWith("__")) return undefined;

	const declarations = symbol.getDeclarations() ?? [];
	const contractDeclarations = declarations.filter(
		(declaration) =>
			ts.isInterfaceDeclaration(declaration) || ts.isClassDeclaration(declaration) || ts.isTypeAliasDeclaration(declaration),
	);
	if (contractDeclarations.length === 0 || checker.getPropertiesOfType(type).length === 0) return undefined;
	if (contractDeclarations.every((declaration) => program.isSourceFileDefaultLibrary(declaration.getSourceFile()))) {
		return undefined;
	}
	return symbol;
}

function createContractCollector(program, checker) {
	const cache = new Map();

	return (rootType) => {
		const cached = cache.get(rootType);
		if (cached) return cached;

		const contracts = [];
		const seenTypes = new Set();
		const seenContracts = new Set();

		function visit(type) {
			if (!type || seenTypes.has(type)) return;
			seenTypes.add(type);

			const symbol = getContractSymbol(program, checker, type);
			if (symbol && !seenContracts.has(symbol)) {
				seenContracts.add(symbol);
				contracts.push({ symbol, type });
			}

			if (type.isUnionOrIntersection()) {
				for (const member of type.types) visit(member);
			}

			for (const typeArgument of type.aliasTypeArguments ?? []) visit(typeArgument);
			if (type.flags & ts.TypeFlags.Object && type.objectFlags & ts.ObjectFlags.Reference) {
				for (const typeArgument of checker.getTypeArguments(type)) visit(typeArgument);
			}

			if (symbol) {
				const declaration = symbol.getDeclarations()?.[0];
				if (declaration && program.isSourceFileDefaultLibrary(declaration.getSourceFile())) return;
			}

			if (type.flags & ts.TypeFlags.Object) {
				for (const property of checker.getPropertiesOfType(type)) {
					visit(checker.getTypeOfPropertyOfType(type, property.name));
				}

				if (type.objectFlags & (ts.ObjectFlags.Class | ts.ObjectFlags.Interface)) {
					for (const baseType of checker.getBaseTypes(type)) visit(baseType);
				}
			}
		}

		visit(rootType);
		cache.set(rootType, contracts);
		return contracts;
	};
}

function createTypeContainmentChecker(checker) {
	return (rootType, containedType) => {
		const seenTypes = new Set();
		const containedProperties = checker.getPropertiesOfType(containedType);

		function visit(type, depth) {
			if (type === containedType) return true;
			if (!type || depth > 6 || seenTypes.has(type)) return false;
			seenTypes.add(type);

			if (
				type.flags & ts.TypeFlags.Object &&
				containedType.flags & ts.TypeFlags.Object &&
				type.objectFlags & ts.ObjectFlags.Anonymous
			) {
				const properties = new Map(checker.getPropertiesOfType(type).map((property) => [property.name, property]));
				if (
					containedProperties.length > 0 &&
					containedProperties.every((containedProperty) => {
						const property = properties.get(containedProperty.name);
						if (!property) return false;
						const declarations = new Set(property.getDeclarations() ?? []);
						return (containedProperty.getDeclarations() ?? []).some((declaration) => declarations.has(declaration));
					})
				) {
					return true;
				}
			}

			if (type.isUnionOrIntersection() && type.types.some((member) => visit(member, depth + 1))) return true;
			if ((type.aliasTypeArguments ?? []).some((typeArgument) => visit(typeArgument, depth + 1))) return true;
			if (
				type.flags & ts.TypeFlags.Object &&
				type.objectFlags & ts.ObjectFlags.Reference &&
				checker.getTypeArguments(type).some((typeArgument) => visit(typeArgument, depth + 1))
			) {
				return true;
			}
			if (!(type.flags & ts.TypeFlags.Object)) return false;

			for (const signature of [
				...checker.getSignaturesOfType(type, ts.SignatureKind.Call),
				...checker.getSignaturesOfType(type, ts.SignatureKind.Construct),
			]) {
				if (visit(checker.getReturnTypeOfSignature(signature), depth + 1)) return true;
			}
			if (!(type.objectFlags & ts.ObjectFlags.Anonymous)) return false;
			for (const property of checker.getPropertiesOfType(type)) {
				const declaration = property.valueDeclaration ?? property.getDeclarations()?.[0];
				if (!declaration || declaration.getSourceFile().isDeclarationFile) continue;
				if (visit(checker.getTypeOfPropertyOfType(type, property.name), depth + 1)) return true;
			}
			return false;
		}

		return visit(rootType, 0);
	};
}

export function analyzeTypeDiscoverability(projectPath = "tsconfig.json", selectedFiles = []) {
	const resolvedProjectPath = resolve(projectPath);
	const configFile = ts.readConfigFile(resolvedProjectPath, ts.sys.readFile);
	if (configFile.error) throw new Error(formatConfigDiagnostics([configFile.error]));

	const parsedConfig = ts.parseJsonConfigFileContent(
		configFile.config,
		ts.sys,
		dirname(resolvedProjectPath),
		undefined,
		resolvedProjectPath,
	);
	if (parsedConfig.errors.length > 0) throw new Error(formatConfigDiagnostics(parsedConfig.errors));

	const program = ts.createProgram(parsedConfig.fileNames, parsedConfig.options);
	const checker = program.getTypeChecker();
	const collectContracts = createContractCollector(program, checker);
	const containsType = createTypeContainmentChecker(checker);
	const rootFiles = new Set(program.getRootFileNames().map((file) => resolve(file)));
	const selectedPaths = selectedFiles.map((file) => {
		const path = resolve(file);
		if (!existsSync(path)) throw new Error(`Selected path does not exist: ${path}`);
		return { path, isDirectory: statSync(path).isDirectory() };
	});
	for (const selection of selectedPaths) {
		const matchingFiles = [...rootFiles].filter((file) =>
			selection.isDirectory ? isPathWithin(selection.path, file) : file === selection.path,
		);
		if (matchingFiles.length === 0) {
			throw new Error(`Selected path contains no files from the TypeScript project: ${selection.path}`);
		}
	}
	const issues = [];

	for (const sourceFile of program.getSourceFiles()) {
		const resolvedFileName = resolve(sourceFile.fileName);
		if (sourceFile.isDeclarationFile || !rootFiles.has(resolvedFileName)) continue;
		if (
			selectedPaths.length > 0 &&
			!selectedPaths.some((selection) =>
				selection.isDirectory
					? isPathWithin(selection.path, resolvedFileName)
					: resolvedFileName === selection.path,
			)
		) {
			continue;
		}

		function visit(node) {
			if (ts.isObjectLiteralExpression(node)) {
				const actualType = checker.getTypeAtLocation(node);
				const directContext = checker.getContextualType(node);
				const directContractSymbols = new Set(
					directContext ? collectContracts(directContext).map((contract) => contract.symbol) : [],
				);

				let ancestor = node.parent;
				let issue;
				while (ancestor && !ts.isSourceFile(ancestor)) {
					if (ts.isExpression(ancestor)) {
						if (!containsType(checker.getTypeAtLocation(ancestor), actualType)) break;
						const contextualType = checker.getContextualType(ancestor);
						if (contextualType) {
							const contextualContracts = collectContracts(contextualType);
							const hasDirectAssociation = contextualContracts.some(
								(contract) =>
									directContractSymbols.has(contract.symbol) && checker.isTypeAssignableTo(actualType, contract.type),
							);
							if (hasDirectAssociation) break;

							for (const contract of contextualContracts) {
								if (!checker.isTypeAssignableTo(actualType, contract.type)) continue;
								issue = contract;
								break;
							}
						}
					}
					if (issue) break;
					ancestor = ancestor.parent;
				}

				if (issue) {
					const position = sourceFile.getLineAndCharacterOfPosition(node.getStart(sourceFile));
					issues.push({
						fileName: sourceFile.fileName,
						line: position.line + 1,
						character: position.character + 1,
						target: checker.symbolToString(issue.symbol, node, ts.SymbolFlags.Type),
					});
				}
			}

			ts.forEachChild(node, visit);
		}

		visit(sourceFile);
	}

	return issues;
}

function printUsage() {
	console.log(`Usage: node scripts/check-type-discoverability.mjs [options] [files...]

Options:
  -p, --project <path>  TypeScript configuration (default: tsconfig.json)
  -h, --help            Show this help

When files or directories are provided, only matching project files are checked. Directories are recursive.
The check follows values through enclosing expressions, but not through separate variable or function declarations.`);
}

const isMain = process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
	let projectPath = "tsconfig.json";
	const selectedFiles = [];
	let showHelp = false;

	for (let index = 2; index < process.argv.length; index++) {
		const argument = process.argv[index];
		if (argument === "--help" || argument === "-h") {
			showHelp = true;
			continue;
		}
		if (argument === "--project" || argument === "-p") {
			const value = process.argv[++index];
			if (!value) {
				console.error(`${argument} requires a path`);
				process.exit(2);
			}
			projectPath = value;
			continue;
		}
		if (argument.startsWith("-")) {
			console.error(`Unknown option: ${argument}`);
			process.exit(2);
		}
		selectedFiles.push(argument);
	}

	if (showHelp) {
		printUsage();
	} else if (!existsSync(projectPath)) {
		console.error(`TypeScript configuration not found: ${projectPath}`);
		process.exitCode = 2;
	} else {
		try {
			const issues = analyzeTypeDiscoverability(projectPath, selectedFiles);
			if (issues.length > 0) {
				console.error("Object literals without a discoverable named type association:");
				for (const issue of issues) {
					const fileName = relative(process.cwd(), issue.fileName) || issue.fileName;
					console.error(
						`  ${fileName}:${issue.line}:${issue.character}: structurally consumed as ${issue.target}; add a direct type annotation, generic argument, or satisfies expression`,
					);
				}
				console.error(`Found ${issues.length} object literal${issues.length === 1 ? "" : "s"}.`);
				process.exitCode = 1;
			} else {
				console.log("No object literals without a discoverable named type association found.");
			}
		} catch (error) {
			console.error(error instanceof Error ? error.message : String(error));
			process.exitCode = 2;
		}
	}
}
