/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.protocol.natives.thrift5;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.compiler.support.JavassistCompiler;

import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import org.codehaus.janino.SimpleCompiler;

import static org.apache.dubbo.rpc.protocol.natives.thrift5.Thrift5Protocol.THRIFT_ASYNC_IFACE;
import static org.apache.dubbo.rpc.protocol.natives.thrift5.Thrift5Protocol.THRIFT_IFACE;

/**
 * 直接代理.
 */
public class ThriftPoolDirectProxy {
    static JavassistCompiler compiler = new JavassistCompiler();

    static String PACKAGE = "package %s; \n";
    static String DEFAULT_IMPORT =
                      "import org.apache.dubbo.rpc.protocol.natives.thrift5.*;\n"
                    + "import java.util.concurrent.*;\n"
                    + "import java.lang.*;\n";

    static String IMPORT = "import %s; \n";
    static String CLASS = "public class ThriftPoolDirectProxy$%s implements %s { \n";
    static String PRO1 = "org.apache.dubbo.rpc.protocol.natives.thrift5.ThriftGenericKeyedObjectPool pool;\n";
    static String PRO2 = "org.apache.dubbo.rpc.protocol.natives.thrift5.ThriftGenericKeyedObjectPool.EndPoint key;\n";
    static String PRO3 = "ThriftInterceptor thriftInterceptor = new ThriftInterceptor();\n";

    static String C =
            "public ThriftPoolDirectProxy$%s(org.apache.dubbo.rpc.protocol.natives.thrift5.ThriftGenericKeyedObjectPool pool, org.apache.dubbo.rpc.protocol.natives.thrift5.ThriftGenericKeyedObjectPool.EndPoint key) { \n"
                    + "        this.pool = pool;"
                    + "        this.key = key;"
                    + "    }";

    static String METHOD = "\npublic %s %s( %s )";
    static String METHOD_E = " {";
    static String METHOD_I =
              " Exception errObj = null;" + "       "
            + " try { \n"
            + "     thriftInterceptor.before(key, \"%s\", %s);\n";

    static String METHOD_I_RETURN =
            "       Object res = Thrift5ExecutorHolder.exec(new Callable<Object>() {\n" + "           "
            + "          @Override\n"
            + "          public Object call() throws %s {\n"
            + "          Exception errInnerObj = null;    "
            + "          final Object client = thriftInterceptor.borrowClient(pool, key);   "
            + "          try{  return ((%s)client).%s( %s ) ; } catch (Exception e) { errInnerObj = e; throw e; }"
            + "          finally { thriftInterceptor.recycleClient(pool, key, client, errInnerObj); }\n"
            + "       }\n"
            + "     });"
            + "    thriftInterceptor.success(key, \"%s\", res, %s); return %s ; \n";

    static String METHOD_I_VOID =
            "      Thrift5ExecutorHolder.exec(new Runnable() {\n"
            + "          @Override\n"
            + "          public void run() throws %s\n{ "
            + "          Exception errInnerObj = null;  "
            + "          final Object client = thriftInterceptor.borrowClient(pool, key);   "
            + "          try{   ((%s)client).%s( %s ); } catch (Exception e) { errInnerObj = e; throw e; } "
            + "          finally { thriftInterceptor.recycleClient(pool, key, client, errInnerObj); }\n"
            + "       }\n"
            + "     });"
            + "    thriftInterceptor.success(key, \"%s\", void.class, %s); return ; \n";

    static String METHOD_I3 =
            "  } catch (Exception e) {\n"
            + "     e.printStackTrace();\n"
            + "     thriftInterceptor.error(key, \"%s\", e); errObj = e; \n"
            + "     throw new RuntimeException(e);\n"
            + " } finally {\n"
            + "     thriftInterceptor.over(key, \"%s\");\n"
            + " }";
    static String METHOD_END = "}";

    static String CLASS_END = "}";

    public static Class directProxy(Class type) throws Throwable {
        String typeName = type.getName();
        String packageName = type.getPackage().getName();
        int idx = Math.max(typeName.indexOf(THRIFT_ASYNC_IFACE), typeName.indexOf(THRIFT_IFACE));
        String serviceClassName = typeName.substring(0, idx);
        String simpleClassName = serviceClassName.substring(serviceClassName.lastIndexOf(".") + 1);
//        String asyncIfaceClassName = serviceClassName + THRIFT_ASYNC_IFACE;
//        Class<?> asyncIfaceClass = Class.forName(asyncIfaceClassName);
        String syncIfaceClassName = serviceClassName + THRIFT_IFACE;
        Class<?> syncIfaceClass = Class.forName(syncIfaceClassName);

        String syncIfaceClassNameCode = syncIfaceClassName.replace("$", ".");
//        String asyncIfaceClassNameCode = asyncIfaceClassName.replace("$", ".");
        // 生产代码.
        StringBuilder code = new StringBuilder();
        code.append(String.format(PACKAGE, packageName));
        code.append(DEFAULT_IMPORT);
        code.append(String.format(IMPORT, syncIfaceClassNameCode));
        code.append(String.format(CLASS, simpleClassName, syncIfaceClassNameCode));
//        code.append(String.format(IMPORT, asyncIfaceClassNameCode));
//        code.append(String.format(CLASS, simpleClassName, syncIfaceClassNameCode + "," + asyncIfaceClassNameCode));
        //        code.append(String.format(LOG,simpleClassName));

        code.append(PRO1);
        code.append(PRO2);
        code.append(PRO3);

        code.append(String.format(C, simpleClassName));

        // 实现抽象方法
        List<Method> declaredMethods = getMethods(syncIfaceClass);
        processMethods(declaredMethods, code, syncIfaceClassNameCode);
//        List<Method> declaredMethods2 = getMethods(asyncIfaceClass);
//        processMethods(declaredMethods2, code, asyncIfaceClassNameCode);
        code.append(CLASS_END);
        SimpleCompiler compiler = new SimpleCompiler();
        compiler.setParentClassLoader(type.getClassLoader());
        System.out.println(code.toString());
        // 编译源代码
        compiler.cook(new StringReader(code.toString()));
        // 加载编译后的类
        Class<?> proxyClass =
                compiler.getClassLoader().loadClass(packageName + ".ThriftPoolDirectProxy$" + simpleClassName);
        return proxyClass;
    }

    private static void processMethods(List<Method> declaredMethods, StringBuilder code, String faceClassNameCode) {
        for (Method method : declaredMethods) {
            if (Modifier.isAbstract(method.getModifiers())) {
                String throwsClause = getThrowsClause(method);
                // 获取返回类型;
                Class<?> returnType = method.getReturnType();
                String methodName = method.getName();
                // 获取参数类型
                Class<?>[] parameterTypes = method.getParameterTypes();
                StringBuilder pcs = new StringBuilder();
                StringBuilder pcObjs = new StringBuilder();
                String arg = "arg";
                int position = 0;
                for (Class pc : parameterTypes) {
                    if (position != 0) {
                        pcs.append(",");
                        pcObjs.append(",");
                    }
                    pcs.append("final ");
                    pcs.append(pc.getName());
                    pcs.append(" ");
                    pcs.append(arg);
                    pcs.append(position);

                    pcObjs.append(arg);
                    pcObjs.append(position);

                    position++;
                }
                // 拼装代码
                code.append(String.format(METHOD, returnType.getName(), methodName, pcs));
                code.append(METHOD_E);
                code.append(String.format(METHOD_I, methodName, (pcObjs.length() == 0 ? "null" : pcObjs)));
                if (returnType == Void.class || returnType == void.class) {
                    code.append(
                            String.format(METHOD_I_VOID, throwsClause, faceClassNameCode, methodName, pcObjs, methodName, (pcObjs.length() == 0 ? "null" : pcObjs)));
                } else {
                    code.append(String.format(
                            METHOD_I_RETURN,
                            throwsClause,
                            faceClassNameCode,
                            methodName,
                            pcObjs,
                            methodName,
                            (pcObjs.length() == 0 ? "null" : pcObjs),
                            unboxExpr(returnType.getName())));
                }
                code.append(String.format(METHOD_I3, methodName, methodName));
                code.append(METHOD_END);
            }
        }
    }

    public static String getThrowsClause(Method method) {
        Class<?>[] exTypes = method.getExceptionTypes();
        if (exTypes.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder("java.lang.Exception");
        for (int i = 0; i < exTypes.length; i++) {
            if(exTypes[i] == Exception.class){
                continue;
            }
            if (sb.length() > 0) sb.append(", ");
            sb.append(exTypes[i].getName());   // 用全限定名，Janino 最稳
        }
        return sb.toString();
    }

    public static String unboxExpr(String typeName) {
        switch (typeName) {
            case "int":     return "((Integer) res).intValue()";
            case "long":    return "((Long) res).longValue()";
            case "short":   return "((Short) res).shortValue()";
            case "byte":    return "((Byte) res).byteValue()";
            case "char":    return "((Character) res).charValue()";
            case "boolean": return "((Boolean) res).booleanValue()";
            case "float":   return "((Float) res).floatValue()";
            case "double":  return "((Double) res).doubleValue()";
            default:
                // 引用类型（String、List 等）直接强转
                return "(" + typeName + ") res";
        }
    }

    public static <T> T getProxyObject(Class clazz, URL url, BiFunction<Class, URL, Object> processRefer)
            throws Exception {
        try {
            Class proxyClass = directProxy(clazz);
            Constructor declaredConstructors = proxyClass.getDeclaredConstructor(
                    ThriftGenericKeyedObjectPool.class, ThriftGenericKeyedObjectPool.EndPoint.class);
            ThriftGenericKeyedObjectPool instance = ThriftGenericKeyedObjectPool.getInstance(processRefer, url);
            ThriftGenericKeyedObjectPool.EndPoint endPoint = new ThriftGenericKeyedObjectPool.EndPoint(clazz, url);
            Object o = declaredConstructors.newInstance(instance, endPoint);
            return (T) o;
        } catch (Throwable throwable) {
            // 这里可以退化成动态代理
            throwable.printStackTrace();
        }
        return ThriftPoolProxy.getProxyObject(clazz, url, processRefer);
    }

    public static List<Method> getMethods(Class clazz) {
        Set<Class> extendsInterfaces = getExtendsInterfaces(clazz);
        extendsInterfaces.add(clazz);
        ArrayList<Method> list = new ArrayList<>();

        for(Class c : extendsInterfaces){
            Method[] declaredMethods = c.getDeclaredMethods();
            for (Method method : declaredMethods) {
                list.add(method);
            }
        }

        return list;
    }

    public static Set<Class> getExtendsInterfaces(Class clazz) {
        Set<Class> list = new HashSet<>();
        if(!clazz.isInterface()) {
            return list;
        }
        Class[] interfaces = clazz.getInterfaces();
        for (Class suClass : interfaces) {
            list.add(suClass);
            list.addAll(getExtendsInterfaces(suClass));
        }

        return list;
    }
}
